from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") == 229)
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .filter(F.col("total_quantity") >= 630)
    .select("product_id", "total_quantity")
    .orderBy(F.col("product_id").desc())
)
