from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter(F.col("end_date") >= F.lit("2026-02-16").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
        F.round(F.col("premium") * 1.07, 2).alias("premium_adj_2"),
    )
)

enriched = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

result = (
    enriched
    .groupingSets(
        [["status", "holder_id", "holder_segment"], ["status"]],
        "status", "holder_id", "holder_segment",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("premium", 0.5).alias("median_premium"),
        F.round(F.sum("premium") / F.nullif(F.sum("holder_risk_score"), F.lit(0)), 3).alias("premium_to_holder_risk_score_ratio"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "holder_id",
        F.coalesce(F.col("holder_segment"), F.lit("ALL")).alias("holder_segment"),
        "cnt",
        "median_premium",
        "premium_to_holder_risk_score_ratio",
    )
    .orderBy(
        F.col("status").asc_nulls_last(),
        F.col("holder_id").asc_nulls_last(),
        F.col("holder_segment").asc_nulls_last(),
    )
)
