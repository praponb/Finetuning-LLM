from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 4)
    .groupBy(
        F.when(F.col("quantity") >= 217, "high").when(F.col("quantity") >= 72, "mid").otherwise("low").alias("quantity_bucket"),
        "store_id",
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("inventory_count"),
    )
    .filter(F.col("max_quantity") >= 230)
    .select("quantity_bucket", "store_id", "max_quantity", "inventory_count")
    .orderBy(F.col("quantity_bucket").desc(), "store_id")
    .limit(10)
)
