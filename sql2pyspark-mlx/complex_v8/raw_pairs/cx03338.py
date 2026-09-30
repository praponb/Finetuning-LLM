from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_holder_id_by_policy_e = Window.partitionBy("policy_holder_id").orderBy(F.col("policy_end_date").desc())
w_policy_holder_id_by_holder_j = Window.partitionBy("policy_holder_id").orderBy("holder_joined_on", "id")
w_claim_type_policy_holder_id = Window.partitionBy("claim_type", "policy_holder_id").orderBy("holder_joined_on", "id")
w_all_by_holder_joined_on = Window.orderBy(F.col("holder_joined_on").desc_nulls_first(), "id")
w_claim_type_policy_holder_id_2 = Window.partitionBy("claim_type", "policy_holder_id").orderBy("holder_joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_holder_id_by_holder_j_2 = Window.partitionBy("policy_holder_id").orderBy("holder_joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_holder_id_by_policy_e_2 = Window.partitionBy("policy_holder_id").orderBy(F.col("policy_end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_holder_id_by_holder_j_3 = Window.partitionBy("policy_holder_id").orderBy("holder_joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_holder_id_by_policy_e_3 = Window.partitionBy("policy_holder_id").orderBy(F.col("policy_end_date").desc(), "id")
w_holder_joined_on_month = Window.partitionBy(F.month("holder_joined_on"))
w_all_by_holder_joined_on_cume = Window.orderBy(F.col("holder_joined_on_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_first_known_reported = Window.orderBy("first_known_reported_at", "id")

filtered_claims = (
    df_claims
    .filter(F.col("approved_amount") >= 0.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.concat_ws(" / ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

enriched = (
    filtered_claims.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.premium") >= 2033.8), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .filter(F.col("p.holder_id").isNotNull())
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.status",
        "s.reported_at",
        "s.status_claim_type_label",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("po.joined_on").alias("holder_joined_on"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "reported_at",
        "status_claim_type_label",
        "policy_end_date",
        "policy_product",
        "policy_holder_id",
        "holder_joined_on",
        F.rank().over(w_policy_holder_id_by_policy_e).alias("policy_end_date_rank"),
        F.lag("reported_at").over(w_policy_holder_id_by_holder_j).alias("prev_reported_at"),
        F.round(F.cume_dist().over(w_claim_type_policy_holder_id), 3).alias("holder_joined_on_cume_dist"),
        F.dense_rank().over(w_all_by_holder_joined_on).alias("holder_joined_on_dense_rank"),
        F.nth_value("holder_joined_on", 2).over(w_claim_type_policy_holder_id_2).alias("second_holder_joined_on"),
        F.first("holder_joined_on").over(w_policy_holder_id_by_holder_j_2).alias("first_holder_joined_on"),
        F.nth_value("reported_at", 2).over(w_policy_holder_id_by_policy_e_2).alias("second_reported_at"),
        F.first("reported_at", ignorenulls=True).over(w_policy_holder_id_by_holder_j_3).alias("first_known_reported_at"),
        F.lag("reported_at", 3).over(w_policy_holder_id_by_policy_e_3).alias("prev3_reported_at"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "adjuster_id",
        "policy_end_date",
        "policy_product",
        "policy_holder_id",
        "holder_joined_on",
        "prev_reported_at",
        "holder_joined_on_dense_rank",
        "second_holder_joined_on",
        "first_holder_joined_on",
        "second_reported_at",
        "first_known_reported_at",
        "prev3_reported_at",
        F.round(F.col("holder_joined_on_dense_rank") / F.nullif(F.sum("holder_joined_on_dense_rank").over(w_holder_joined_on_month), F.lit(0)), 4).alias("holder_joined_on_dense_rank_share_of_holder_join"),
        F.last("second_reported_at", ignorenulls=True).over(w_all_by_holder_joined_on_cume).alias("last_known_second_reported_at"),
        F.round(F.avg("policy_end_date_rank").over(w_holder_joined_on_month), 2).alias("holder_joined_on_month_avg_policy_end_date_rank"),
        F.row_number().over(w_all_by_first_known_reported).alias("rn"),
        F.last("holder_joined_on_cume_dist", ignorenulls=True).over(w_all_by_holder_joined_on_cume).alias("last_known_holder_joined_on_cume_dist"),
        F.min("holder_joined_on_cume_dist").over(w_holder_joined_on_month).alias("holder_joined_on_month_min_holder_joined_on_cume"),
        F.max("holder_joined_on_dense_rank").over(w_holder_joined_on_month).alias("holder_joined_on_month_max_holder_joined_on_dens"),
    )
)

result = (
    metrics
    .select(
        "id",
        "adjuster_id",
        "policy_product",
        "policy_holder_id",
        "holder_joined_on",
        "prev_reported_at",
        "holder_joined_on_dense_rank",
        "second_holder_joined_on",
        "first_holder_joined_on",
        "second_reported_at",
        "first_known_reported_at",
        "prev3_reported_at",
        "holder_joined_on_dense_rank_share_of_holder_join",
        "holder_joined_on_month_avg_policy_end_date_rank",
        "rn",
        "holder_joined_on_month_min_holder_joined_on_cume",
        F.round((F.unix_timestamp(F.col("first_known_reported_at")) - F.unix_timestamp(F.col("second_reported_at"))) / 3600, 1).alias("hours_second_reported_at_to_first_known_reported"),
        F.length(F.trim(F.col("policy_product"))).alias("policy_product_len"),
    )
    .orderBy("policy_product_len", "prev3_reported_at")
)
