from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter(~(F.col("region").isNull() | (F.col("opened_date") >= "2025-05-01")))
)

stores_stats = (
    valid_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "Central", 1).otherwise(0))) / F.count("*")), 1).alias("pct_central"),
    )
)

result = (
    stores_stats
    .filter(F.col("pct_central") >= 44)
    .select("opened_year", "pct_central")
    .orderBy("opened_year", "pct_central")
    .limit(20)
)
