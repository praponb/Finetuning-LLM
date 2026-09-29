from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", F.quarter("last_restock").alias("last_restock_quarter"))
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .filter(F.col("quantity_range") >= 36)
)
