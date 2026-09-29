from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"), "product_id")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 1)
    .orderBy(F.col("last_restock_quarter").desc(), F.col("product_id").desc())
)
