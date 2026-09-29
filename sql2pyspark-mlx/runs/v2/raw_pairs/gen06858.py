from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.col("region").isNotNull())

stores_stats = (
    valid_stores
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "East", 1).otherwise(0))) / F.count("*")), 1).alias("pct_east"),
    )
)

result = (
    stores_stats
    .filter(F.col("pct_east") < 33)
    .select("city", "pct_east")
)
