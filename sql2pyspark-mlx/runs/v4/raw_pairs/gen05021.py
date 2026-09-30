from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 9]))
    .groupBy("store_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("avg_quantity") > 240)
    .select("store_id", "avg_quantity", "max_quantity")
    .orderBy(F.col("store_id").desc())
    .limit(20)
)
