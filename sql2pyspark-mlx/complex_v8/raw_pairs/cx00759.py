from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_end_date_month = Window.partitionBy(F.month("policy_end_date"))
w_all_by_claimed_amount = Window.orderBy(F.col("claimed_amount").desc(), "id")
w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy(F.col("claimed_amount").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_status_adjuster_id = Window.partitionBy("status", "adjuster_id")
w_all_by_claimed_amount_full = Window.orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount_movin = Window.orderBy("approved_amount", "id").rowsBetween(-3, Window.currentRow)
w_policy_product_policy_id = Window.partitionBy("policy_product", "policy_id")
w_all_by_adjuster_level_vs_pol = Window.orderBy("adjuster_level_vs_policy_end_date_month_avg", "id")
w_all_by_policy_end_date_runni = Window.orderBy(F.col("policy_end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_claims = (
    df_claims
    .filter((F.col("claimed_amount").between(6745.1, 32441.2)) & (F.col("claim_date").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
    )
)

enriched = (
    recent_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        F.col("p.product").alias("policy_product"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("a.level").alias("adjuster_level"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "policy_product",
        "policy_end_date",
        "policy_holder_id",
        "adjuster_level",
        F.size(F.collect_set("policy_holder_id").over(w_policy_end_date_month)).alias("distinct_policy_holder_id_in_policy_end_date_mon"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_end_date_month), F.lit(0)), 4).alias("claimed_amount_share_of_policy_end_date_month"),
        F.dense_rank().over(w_all_by_claimed_amount).alias("claimed_amount_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_claimed_amount), 3).alias("claimed_amount_percent_rank"),
        F.max("approved_amount").over(w_policy_id_by_claimed_amount).alias("approved_amount_max_3"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_policy_end_date_month), 2).alias("adjuster_level_vs_policy_end_date_month_avg"),
        F.size(F.collect_set("policy_id").over(w_status_adjuster_id)).alias("distinct_policy_id_in_status_adjuster_id"),
        F.last("policy_end_date").over(w_all_by_claimed_amount_full).alias("last_policy_end_date"),
        F.round(F.avg("claimed_amount").over(w_all_by_approved_amount_movin), 2).alias("claimed_amount_avg_4"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_status_adjuster_id), F.lit(0)), 4).alias("approved_amount_share_of_status_adjuster_id"),
    )
)

with_trends = (
    metrics
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "policy_product",
        "policy_end_date",
        "policy_holder_id",
        "adjuster_level",
        "claimed_amount_dense_rank",
        "claimed_amount_percent_rank",
        "approved_amount_max_3",
        "adjuster_level_vs_policy_end_date_month_avg",
        "distinct_policy_id_in_status_adjuster_id",
        "claimed_amount_avg_4",
        "approved_amount_share_of_status_adjuster_id",
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_policy_product_policy_id), 2).alias("adjuster_level_vs_policy_product_policy_id_avg"),
        F.round(F.col("claimed_amount_avg_4") / F.nullif(F.sum("claimed_amount_avg_4").over(w_policy_product_policy_id), F.lit(0)), 4).alias("claimed_amount_avg_4_share_of_policy_product_pol"),
        F.lag("adjuster_level", 3).over(w_all_by_adjuster_level_vs_pol).alias("prev3_adjuster_level"),
        F.min("adjuster_level").over(w_all_by_policy_end_date_runni).alias("min_adjuster_level_so_far"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "policy_product",
        "policy_holder_id",
        "adjuster_level",
        "claimed_amount_dense_rank",
        "approved_amount_max_3",
        "adjuster_level_vs_policy_end_date_month_avg",
        "distinct_policy_id_in_status_adjuster_id",
        "approved_amount_share_of_status_adjuster_id",
        "adjuster_level_vs_policy_product_policy_id_avg",
        "claimed_amount_avg_4_share_of_policy_product_pol",
        "prev3_adjuster_level",
        F.least(F.col("prev3_adjuster_level"), F.col("claimed_amount_avg_4")).alias("least_prev3_adjuster_level_claimed_amount_avg_4"),
        F.round(F.col("approved_amount_max_3") / F.nullif(F.col("claimed_amount_avg_4"), F.lit(0)), 3).alias("approved_amount_max_3_per_claimed_amount_avg_4"),
    )
    .orderBy("id", "adjuster_level_vs_policy_end_date_month_avg")
)
