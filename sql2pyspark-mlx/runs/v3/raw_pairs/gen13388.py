from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(F.max("quantity").alias("max_quantity"))
    .orderBy(F.col("max_quantity").desc(), "last_restock_month")
    .limit(1)
)
