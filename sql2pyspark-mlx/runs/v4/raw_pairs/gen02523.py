from pyspark.sql import functions as F

filtered_inventory = df_inventory.filter(F.col("product_id").isin([2, 9]))

store_id_summary = (
    filtered_inventory
    .groupBy(
        "store_id",
        F.when(F.col("quantity") >= 222, "high").when(F.col("quantity") >= 82, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    store_id_summary
    .filter(F.col("avg_quantity") < 27)
    .select("store_id", "quantity_bucket", "avg_quantity", "quantity_count")
    .orderBy("store_id")
)
