from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("opened_date").between("2023-07-01", "2024-03-21") & F.col("sqft").isNotNull())
)

stores_stats = (
    base
    .groupBy("region")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "West", 1).otherwise(0))) / F.count("*")), 1).alias("pct_west"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
    )
)

result = (
    stores_stats
    .select("region", "pct_bangkok")
    .orderBy(F.col("pct_bangkok").desc(), "region")
)
