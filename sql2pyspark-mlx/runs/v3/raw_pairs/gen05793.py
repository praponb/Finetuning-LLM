from pyspark.sql import functions as F

src = (
    df_stores
    .filter((F.col("city") == "New York") & (F.col("opened_date") > "2024-11-01"))
)

agg = (
    src
    .groupBy("region")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
        F.sum(F.when(F.col("region") == "East", 1).otherwise(0)).alias("east_count"),
    )
)

result = (
    agg
    .filter(F.col("east_count") > 67)
    .select("region", "pct_london", "east_count")
    .orderBy(F.col("region").desc())
)
