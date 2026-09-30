from pyspark.sql import functions as F

src = (
    df_inventory
    .filter(F.col("store_id").isin([2, 4, 5, 6]) & (F.col("last_restock") < "2024-03-01") & (F.col("quantity") == 108))
    .withColumn("quantity_band", F.when(F.col("quantity") >= 267, "High").when(F.col("quantity") >= 99, "Medium").otherwise("Low"))
)

summary = (
    src
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    summary
    .select("last_restock_quarter", "quantity_count", "min_quantity")
    .orderBy(F.col("min_quantity").desc())
)
