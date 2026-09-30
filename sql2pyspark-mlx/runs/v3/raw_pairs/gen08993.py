from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2025-05-01")
    .withColumn("quantity_rounded", F.round("quantity", 2))
    .withColumn("last_restock_dayofweek", F.dayofweek("last_restock"))
    .orderBy(F.col("last_restock").desc(), F.col("product_id").desc())
)
