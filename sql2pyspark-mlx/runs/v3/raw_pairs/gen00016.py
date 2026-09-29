from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2025-11-01")
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(F.min("quantity").alias("min_quantity"))
)
