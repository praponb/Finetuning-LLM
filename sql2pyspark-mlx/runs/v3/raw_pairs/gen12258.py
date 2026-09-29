from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") <= 172) & ((F.col("store_id") == 5) | (F.col("store_id") == 2)))
    .agg(
        F.sum("quantity").alias("total_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)
