from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-12-20", "2024-05-01"))
    .groupBy(F.year("last_restock").alias("last_restock_year"), "store_id")
    .agg(F.sum("quantity").alias("total_quantity"))
    .filter(F.col("total_quantity") >= 790)
    .orderBy(F.col("store_id").desc(), F.col("last_restock_year").desc())
    .limit(20)
)
