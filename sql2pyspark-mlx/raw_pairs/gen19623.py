from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .orderBy(F.col("store_id").desc(), "product_id")
    .limit(20)
)
