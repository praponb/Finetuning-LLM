from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_claimed_amount_buc = Window.partitionBy("status").orderBy(F.col("claimed_amount_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_claimed_amount_buc_2 = Window.partitionBy("status").orderBy(F.col("claimed_amount_bucket").desc(), "id")
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_policy_id_adjuster_level = Window.partitionBy("policy_id", "adjuster_level")
w_status = Window.partitionBy("status")
w_status_by_approved_amount_pr = Window.partitionBy("status").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-6, -1)
w_policy_id_by_policy_premium = Window.partitionBy("policy_id").orderBy("policy_premium", "id").rowsBetween(-6, -1)
w_status_by_approved_amount_ti = Window.partitionBy("status").orderBy(F.col("approved_amount").desc())
w_all_by_rows_in_policy_id_adj = Window.orderBy(F.col("rows_in_policy_id_adjuster_level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_rows_in_policy_id_adj_2 = Window.orderBy(F.col("rows_in_policy_id_adjuster_level").desc(), "id")
w_all_by_rows_in_policy_id_adj_3 = Window.orderBy(F.col("rows_in_policy_id_adjuster_level").desc())
w_status_by_reported_at_moving = Window.partitionBy("status").orderBy("reported_at", "id").rowsBetween(-6, Window.currentRow)
w_policy_holder_id_distinct_po = Window.partitionBy("policy_holder_id", "distinct_policy_holder_ids").orderBy("distinct_policy_holder_ids", "policy_holder_id")
w_distinct_policy_holder_ids_p = Window.partitionBy("distinct_policy_holder_ids", "policy_holder_id")
w_all_by_distinct_policy_holde = Window.orderBy(F.col("distinct_policy_holder_ids").desc(), "policy_holder_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_claims
    .filter((~F.col("claim_type").isin("theft", "medical")) & (F.col("adjuster_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

combined_details = (
    valid.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .filter(F.col("p.premium") < 4678.8)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claimed_amount_bucket",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

metrics = (
    combined_details
    .select(
        "*",
        F.first("adjuster_level").over(w_status_by_claimed_amount_buc).alias("first_adjuster_level"),
        F.lag("adjuster_level").over(w_status_by_claimed_amount_buc_2).alias("prev_adjuster_level"),
        F.round(F.cume_dist().over(w_all_by_approved_amount), 3).alias("approved_amount_cume_dist"),
        F.size(F.collect_set("adjuster_id").over(w_policy_id_adjuster_level)).alias("distinct_adjuster_id_in_policy_id_adjuster_level"),
        F.round(F.avg("policy_premium").over(w_policy_id_adjuster_level), 2).alias("policy_id_adjuster_level_avg_policy_premium"),
        F.round(F.col("claimed_amount_bucket") - F.avg("claimed_amount_bucket").over(w_status), 2).alias("claimed_amount_bucket_vs_status_avg"),
        F.max("approved_amount").over(w_status_by_approved_amount_pr).alias("approved_amount_max_6_prior"),
        F.sum("approved_amount").over(w_policy_id_by_policy_premium).alias("approved_amount_sum_6_prior"),
        F.max("approved_amount").over(w_status).alias("status_max_approved_amount"),
        F.size(F.collect_set("policy_holder_id").over(w_status)).alias("distinct_policy_holder_id_in_status"),
        F.round(F.col("claimed_amount_bucket") / F.nullif(F.sum("claimed_amount_bucket").over(w_status), F.lit(0)), 4).alias("claimed_amount_bucket_share_of_status"),
        F.dense_rank().over(w_status_by_approved_amount_ti).alias("approved_amount_dense_rank"),
        F.count(F.lit(1)).over(w_policy_id_adjuster_level).alias("rows_in_policy_id_adjuster_level"),
    )
)

metrics_2 = (
    metrics
    .select(
        "id",
        "approved_amount",
        "reported_at",
        "policy_premium",
        "policy_holder_id",
        "adjuster_level",
        "adjuster_name",
        "first_adjuster_level",
        "prev_adjuster_level",
        "distinct_adjuster_id_in_policy_id_adjuster_level",
        "policy_id_adjuster_level_avg_policy_premium",
        "approved_amount_max_6_prior",
        "approved_amount_sum_6_prior",
        "status_max_approved_amount",
        "distinct_policy_holder_id_in_status",
        "claimed_amount_bucket_share_of_status",
        "approved_amount_dense_rank",
        "rows_in_policy_id_adjuster_level",
        F.first("first_adjuster_level", ignorenulls=True).over(w_all_by_rows_in_policy_id_adj).alias("first_known_first_adjuster_level"),
        F.size(F.collect_set("policy_id").over(w_status)).alias("distinct_policy_id_in_status"),
        F.round(F.col("approved_amount_cume_dist") / F.nullif(F.sum("approved_amount_cume_dist").over(w_status), F.lit(0)), 4).alias("approved_amount_cume_dist_share_of_status"),
        F.round(F.cume_dist().over(w_all_by_rows_in_policy_id_adj_2), 3).alias("rows_in_policy_id_adjuster_level_cume_dist"),
        F.rank().over(w_all_by_rows_in_policy_id_adj_3).alias("rows_in_policy_id_adjuster_level_rank"),
        F.round(F.avg("adjuster_level").over(w_status_by_reported_at_moving), 2).alias("adjuster_level_avg_7"),
        F.round(F.avg("first_adjuster_level").over(w_status), 2).alias("status_avg_first_adjuster_level"),
        F.min("approved_amount_cume_dist").over(w_status).alias("status_min_approved_amount_cume_dist"),
    )
)

per_group = (
    metrics_2
    .filter(F.col("policy_holder_id") <= 8)
    .groupBy("policy_holder_id")
    .agg(
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
        F.countDistinct("policy_holder_id").alias("distinct_policy_holder_ids"),
    )
)

windowed = (
    per_group
    .select(
        "policy_holder_id",
        "adjuster_name_list",
        "distinct_policy_holder_ids",
        F.ntile(4).over(w_policy_holder_id_distinct_po).alias("distinct_policy_holder_ids_quartile"),
        F.round(F.avg("distinct_policy_holder_ids").over(w_distinct_policy_holder_ids_p), 2).alias("distinct_policy_holder_ids_policy_holder_id_avg_"),
        F.round(F.col("distinct_policy_holder_ids") / F.nullif(F.sum("distinct_policy_holder_ids").over(w_distinct_policy_holder_ids_p), F.lit(0)), 4).alias("distinct_policy_holder_ids_share_of_distinct_pol"),
        F.first("distinct_policy_holder_ids", ignorenulls=True).over(w_all_by_distinct_policy_holde).alias("first_known_distinct_policy_holder_ids"),
    )
)

result = (
    windowed
    .select(
        "policy_holder_id",
        "adjuster_name_list",
        "distinct_policy_holder_ids_quartile",
        "distinct_policy_holder_ids_policy_holder_id_avg_",
        "first_known_distinct_policy_holder_ids",
        F.size(F.col("adjuster_name_list")).alias("adjuster_name_list_count"),
    )
    .orderBy(
        F.col("adjuster_name_list_count").desc(),
        F.col("distinct_policy_holder_ids_policy_holder_id_avg_").desc(),
    )
)
