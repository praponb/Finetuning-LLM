from pyspark.sql import functions as F

scoped = df_policies.filter((F.col("premium") > 1042.5) & (F.col("product") == "HOME"))

product_stats = (
    scoped
    .groupBy("product")
    .agg(F.avg("premium").alias("avg_premium"))
)

outliers = (
    scoped.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

holder_id_stats = (
    outliers
    .groupBy("holder_id")
    .agg(F.max("premium_minus_avg").alias("max_premium_minus_avg"))
)

vs_group = (
    outliers.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.premium_minus_avg") == F.col("g.max_premium_minus_avg"))
    .select(
        "s.*",
        "g.max_premium_minus_avg",
        F.round(F.col("s.premium_minus_avg") - F.col("g.max_premium_minus_avg"), 2).alias("premium_minus_avg_minus_max"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "premium_minus_avg",
        "premium",
        "avg_premium",
    )
    .unpivot(
        ids=["id"],
        values=["premium_minus_avg", "premium", "avg_premium"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
