from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .filter(F.col("total_quantity") < 470)
    .select("product_id", "store_id", "total_quantity")
    .orderBy(F.col("store_id").desc())
)
