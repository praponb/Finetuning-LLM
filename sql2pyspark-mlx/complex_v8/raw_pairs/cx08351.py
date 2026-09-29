from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product = Window.partitionBy("product")
w_product_by_end_date_moving_2 = Window.partitionBy("product").orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)
w_status_end_date_month_by_pre = Window.partitionBy("status", F.date_trunc("month", "end_date")).orderBy(F.col("premium_adj").desc(), "id")
w_status_end_date_month_by_pre_2 = Window.partitionBy("status", F.date_trunc("month", "end_date")).orderBy(F.col("premium_adj").desc(), "id").rowsBetween(-3, -1)

filtered = (
    df_policies
    .filter(F.col("holder_id") <= 16)
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
        F.abs(F.col("premium") - 3521.8).alias("premium_distance"),
        F.round(F.col("premium") * 1.1, 2).alias("premium_adj"),
    )
)

filtered_2 = (
    filtered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.reported_at") < F.lit("2025-02-24 07:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("product") == "AUTO")
)

screened = (
    filtered_2.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") > 2033.8), "left_semi")
    .filter(F.col("premium_distance").between(1250.0, 3000.0))
)

ranked = (
    screened
    .select(
        "*",
        F.round(F.col("premium_distance") - F.avg("premium_distance").over(w_product), 2).alias("premium_distance_vs_product_avg"),
        F.round(F.avg("premium_adj").over(w_product_by_end_date_moving_2), 2).alias("premium_adj_avg_3"),
        F.round(F.col("premium_distance") / F.nullif(F.sum("premium_distance").over(w_product), F.lit(0)), 4).alias("premium_distance_share_of_product"),
        F.rank().over(w_status_end_date_month_by_pre).alias("premium_adj_rank"),
        F.round(F.col("premium_adj") - F.avg("premium_adj").over(w_product), 2).alias("premium_adj_vs_product_avg"),
        F.round(F.avg("premium_adj").over(w_status_end_date_month_by_pre_2), 2).alias("premium_adj_avg_3_prior"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
    )
)

with_stats = (
    ranked.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.first_claim_reported_at", "c.min_claim_claimed_amount")
)

joined = (
    with_stats.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.product_prefix",
        "s.premium_distance",
        "s.premium_distance_vs_product_avg",
        "s.premium_adj_avg_3",
        "s.premium_adj_rank",
        "s.premium_adj_vs_product_avg",
        "s.premium_adj_avg_3_prior",
        "s.first_claim_reported_at",
        "s.min_claim_claimed_amount",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_details = (
    joined.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.product_prefix",
        "s.premium_adj_avg_3",
        "s.premium_adj_rank",
        "s.premium_adj_avg_3_prior",
        "s.holder_risk_score",
        F.col("p.segment").alias("holder_segment"),
    )
)

result = (
    with_details
    .filter(F.col("premium_adj_rank") > 3)
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "product_prefix",
        "premium_adj_avg_3_prior",
        "holder_segment",
    )
    .orderBy("holder_id")
)
