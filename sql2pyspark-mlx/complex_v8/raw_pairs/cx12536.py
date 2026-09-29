from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter(F.col("holder_id") <= 5)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.date_add(F.col("end_date"), 365).alias("end_date_plus_365d"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

without_matches = (
    filtered_policys
    .crossJoin(filtered_policys.agg(F.max("premium").alias("overall_max_premium")))
    .filter(F.col("premium") < F.col("overall_max_premium"))
    .drop("overall_max_premium")
)

product_stats = (
    without_matches
    .groupBy("product")
    .agg(F.avg("premium").alias("avg_premium"))
)

vs_group = (
    without_matches.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

with_details_holder = (
    vs_group.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.status",
        "s.end_date_plus_365d",
        "s.days_start_date_to_end_date",
        "s.premium_minus_avg",
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
    )
)

result = (
    with_details_holder
    .select(
        "id",
        F.col("premium").alias("m_premium"),
        F.col("premium_minus_avg").alias("premium_minus_avg_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_premium", "premium_minus_avg_v"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
