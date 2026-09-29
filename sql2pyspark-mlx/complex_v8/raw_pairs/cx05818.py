from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_product = Window.partitionBy("policy_product")
w_adjuster_id_by_adjuster_leve = Window.partitionBy("adjuster_id").orderBy(F.col("adjuster_level").asc_nulls_last(), "id")
w_policy_product_adjuster_id_b = Window.partitionBy("policy_product", "adjuster_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_product_by_claim_date = Window.partitionBy("policy_product").orderBy(F.col("claim_date").desc())
w_policy_product_by_claim_date_2 = Window.partitionBy("policy_product").orderBy(F.col("claim_date").desc(), "id")
w_policy_product_by_claim_date_3 = Window.partitionBy("policy_product").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_product_adjuster_id_b_2 = Window.partitionBy("policy_product", "adjuster_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
    )
)

with_details = (
    cleaned.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team").isin("Health", "Travel", "Property")), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .filter(F.length(F.trim(F.col("a.adjuster_name"))) >= 4)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.claim_date_month_end",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("p.product").alias("policy_product"),
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.sum("claimed_amount").over(w_policy_product).alias("policy_product_sum_claimed_amount"),
        F.lead("claim_date_month_end").over(w_adjuster_id_by_adjuster_leve).alias("next_claim_date_month_end"),
        F.last("claimed_amount", ignorenulls=True).over(w_policy_product_adjuster_id_b).alias("last_known_claimed_amount"),
        F.dense_rank().over(w_policy_product_by_claim_date).alias("claim_date_dense_rank"),
        F.size(F.collect_set("status").over(w_policy_product)).alias("distinct_status_in_policy_product"),
        F.row_number().over(w_policy_product_by_claim_date_2).alias("seq_no"),
        F.nth_value("claim_date", 2).over(w_policy_product_by_claim_date_3).alias("second_claim_date"),
        F.count(F.lit(1)).over(w_policy_product).alias("rows_in_policy_product"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_policy_product), 2).alias("adjuster_level_vs_policy_product_avg"),
        F.nth_value("claimed_amount", 2).over(w_policy_product_adjuster_id_b_2).alias("second_claimed_amount"),
        F.dense_rank().over(w_adjuster_id_by_adjuster_leve).alias("adjuster_level_dense_rank"),
    )
)

tagged = (
    with_windows
    .select(
        "*",
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
        F.abs(F.col("rows_in_policy_product") - 10).alias("rows_in_policy_product_distance"),
        F.substring(F.trim(F.col("adjuster_name")), 1, 3).alias("adjuster_name_prefix"),
    )
)

result = (
    tagged
    .filter(F.col("rows_in_policy_product") < 14)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        "claim_date_month_end",
        "adjuster_level",
        "adjuster_name",
        "policy_product",
        "policy_product_sum_claimed_amount",
        "last_known_claimed_amount",
        "claim_date_dense_rank",
        "distinct_status_in_policy_product",
        "seq_no",
        "second_claim_date",
        "rows_in_policy_product",
        "adjuster_level_vs_policy_product_avg",
        "second_claimed_amount",
        "adjuster_level_dense_rank",
        "status_prefix",
        "adjuster_name_prefix",
    )
    .orderBy(
        F.col("claim_date_month_end").desc_nulls_first(),
        F.col("id").asc_nulls_last(),
    )
)
