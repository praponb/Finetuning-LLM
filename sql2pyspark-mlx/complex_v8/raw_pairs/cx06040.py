from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter((F.col("status") == "LAPSED") | (F.col("start_date") < F.lit("2025-03-21").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.abs(F.col("premium") - 2319.8).alias("premium_distance"),
        F.upper(F.trim(F.col("product"))).alias("product_clean"),
    )
)

joined_holder = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.upper(F.trim(F.col("p.holder_name"))).like("N%")), "left")
    .select("s.*", F.col("p.segment").alias("holder_segment"))
)

stats = (
    joined_holder
    .groupBy("holder_id")
    .agg(
        F.round(F.sum("premium_distance") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("premium_distance_to_premium_ratio"),
        F.round(F.sum("premium") / F.nullif(F.sum("premium_distance"), F.lit(0)), 3).alias("premium_to_premium_distance_ratio"),
        F.sum("premium").alias("total_premium"),
        F.avg("premium").alias("avg_premium_check"),
    )
    .filter(F.col("avg_premium_check") > 1541.8)
    .drop("avg_premium_check")
)

result = (
    stats
    .select(
        "holder_id",
        F.col("premium_distance_to_premium_ratio").alias("m_premium_distance_to_premium_ratio"),
        F.col("premium_to_premium_distance_ratio").alias("premium_to_premium_distance_ratio_v"),
    )
    .unpivot(
        ids=["holder_id"],
        values=["m_premium_distance_to_premium_ratio", "premium_to_premium_distance_ratio_v"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
