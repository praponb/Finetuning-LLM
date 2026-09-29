from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_claim_status_by_dis = Window.partitionBy("distinct_claim_status").orderBy(F.col("distinct_claim_status").desc_nulls_first())
w_distinct_claim_status_by_dis_2 = Window.partitionBy("distinct_claim_status").orderBy(F.col("distinct_claim_status").desc_nulls_first(), "claim_date_month").rowsBetween(-1, -1)
w_distinct_claim_status_claim = Window.partitionBy("distinct_claim_status", F.month("claim_date_month"))
w_all_by_stddev_claim_approved = Window.orderBy(F.col("stddev_claim_approved_amount_2").desc(), "claim_date_month").rowsBetween(-6, -1)

cleaned = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

detail_rows = (
    cleaned.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.id")) & (F.col("c.status") == "OPEN"), "inner")
    .select(
        "s.id",
        "s.adjuster_name",
        "s.team",
        "s.level",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

joined = (
    detail_rows.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("c.policy_id"), "left")
    .select(
        "s.id",
        "s.team",
        "s.claim_id",
        "s.claim_status",
        "s.claim_approved_amount",
        "c.claim_date",
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.approved_amount").alias("claim_approved_amount_2"),
        F.col("p.product").alias("policy_product"),
    )
)

grouped = (
    joined
    .filter(F.col("team") != "Motor")
    .groupBy(F.date_trunc("month", "claim_date").alias("claim_date_month"))
    .agg(
        F.sum("claim_approved_amount").alias("total_claim_approved_amount"),
        F.countDistinct("claim_status").alias("distinct_claim_status"),
        F.round(F.stddev("claim_approved_amount_2"), 2).alias("stddev_claim_approved_amount_2"),
    )
)

filtered = (
    grouped
    .crossJoin(grouped.agg(F.avg("total_claim_approved_amount").alias("overall_avg_total_claim_approved_amount")))
    .filter(F.col("total_claim_approved_amount") > F.col("overall_avg_total_claim_approved_amount"))
    .drop("overall_avg_total_claim_approved_amount")
)

metrics = (
    filtered
    .select(
        "*",
        F.round(F.cume_dist().over(w_distinct_claim_status_by_dis), 3).alias("distinct_claim_status_cume_dist"),
        F.round(F.avg("total_claim_approved_amount").over(w_distinct_claim_status_by_dis_2), 2).alias("total_claim_approved_amount_avg_1_prior"),
        F.round(F.col("total_claim_approved_amount") - F.avg("total_claim_approved_amount").over(w_distinct_claim_status_claim), 2).alias("total_claim_approved_amount_vs_distinct_claim_st"),
        F.round(F.col("distinct_claim_status") / F.nullif(F.sum("distinct_claim_status").over(w_distinct_claim_status_claim), F.lit(0)), 4).alias("distinct_claim_status_share_of_distinct_claim_st"),
        F.sum("distinct_claim_status").over(w_distinct_claim_status_claim).alias("distinct_claim_status_claim_date_month_month_sum"),
        F.sum("distinct_claim_status").over(w_all_by_stddev_claim_approved).alias("distinct_claim_status_sum_6_prior"),
    )
)

result = (
    metrics
    .select(
        "claim_date_month",
        "total_claim_approved_amount",
        "distinct_claim_status",
        "stddev_claim_approved_amount_2",
        "total_claim_approved_amount_vs_distinct_claim_st",
        "distinct_claim_status_share_of_distinct_claim_st",
        "distinct_claim_status_claim_date_month_month_sum",
        "distinct_claim_status_sum_6_prior",
    )
    .orderBy(
        F.col("distinct_claim_status_sum_6_prior").desc_nulls_first(),
        "stddev_claim_approved_amount_2",
    )
)
