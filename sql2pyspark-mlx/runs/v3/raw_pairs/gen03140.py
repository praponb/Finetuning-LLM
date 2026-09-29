from pyspark.sql import functions as F

src = df_inventory.filter(F.col("last_restock") >= "2025-03-19")

product_id_summary = (
    src
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    product_id_summary
    .filter(F.col("total_quantity") >= 630)
    .orderBy(F.col("total_quantity").desc(), F.col("product_id").desc())
)
