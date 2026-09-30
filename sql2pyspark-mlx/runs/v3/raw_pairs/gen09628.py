from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(F.col("region").isNull())
    .withColumn("store_name_normalized", F.upper(F.trim("store_name")))
)

summary = (
    recent_stores
    .groupBy(
        "region",
        F.when(F.col("region").isin(["Central", "West"]), "core").otherwise("other").alias("region_group"),
    )
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.round(((100 * F.sum(F.when(F.col("city") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
)

result = (
    summary
    .filter(F.col("pct_new_york") >= 48)
    .select("region", "region_group", "last_opened_date", "pct_new_york")
)
