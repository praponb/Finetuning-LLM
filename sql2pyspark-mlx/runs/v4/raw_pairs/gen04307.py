from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 5, 7, 9]))
    .groupBy("store_id")
    .agg(F.max("quantity").alias("max_quantity"))
    .filter(F.col("max_quantity") >= 200)
    .select("store_id", "max_quantity")
    .orderBy("store_id")
)
