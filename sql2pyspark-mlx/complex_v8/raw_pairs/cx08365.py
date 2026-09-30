from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_by_start_date = Window.partitionBy("product").orderBy(F.col("start_date").asc_nulls_last(), "id")
w_holder_region = Window.partitionBy("holder_region")

filtered = (
    df_policies
    .filter(F.col("status").isin("EXPIRED", "LAPSED"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket_2"),
    )
)

enriched = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score") > 0.2), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.product_or_unknown",
        "s.premium_bucket_2",
        F.col("p.region").alias("holder_region"),
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "product_or_unknown",
        "premium_bucket_2",
        "holder_region",
        "holder_segment",
        "holder_risk_score",
        (F.col("holder_risk_score") - F.lag("holder_risk_score").over(w_product_by_start_date)).alias("holder_risk_score_change"),
        F.sum("holder_risk_score").over(w_holder_region).alias("holder_region_sum_holder_risk_score"),
        F.round(F.avg("premium_bucket_2").over(w_holder_region), 2).alias("holder_region_avg_premium_bucket_2"),
        F.count(F.lit(1)).over(w_holder_region).alias("rows_in_holder_region"),
        F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_holder_region), 2).alias("holder_risk_score_vs_holder_region_avg"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.max("claimed_amount").alias("max_claim_claimed_amount"),
    )
)

summarized_rows = (
    with_trends.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.first_claim_reported_at", "c.max_claim_claimed_amount")
)

result = (
    summarized_rows
    .rollup("holder_id", "holder_region")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("premium_bucket_2") / F.nullif(F.sum("holder_risk_score_vs_holder_region_avg"), F.lit(0)), 3).alias("premium_bucket_2_to_holder_risk_score_vs_holder_"),
        F.round(F.sum("holder_risk_score_change") / F.nullif(F.sum("holder_risk_score_vs_holder_region_avg"), F.lit(0)), 3).alias("holder_risk_score_change_to_holder_risk_score_vs"),
        F.percentile_approx("rows_in_holder_region", 0.5).alias("median_rows_in_holder_region"),
    )
    .select(
        "holder_id",
        F.coalesce(F.col("holder_region"), F.lit("ALL")).alias("holder_region"),
        "grouping_level",
        "premium_bucket_2_to_holder_risk_score_vs_holder_",
        "holder_risk_score_change_to_holder_risk_score_vs",
        "median_rows_in_holder_region",
    )
)
