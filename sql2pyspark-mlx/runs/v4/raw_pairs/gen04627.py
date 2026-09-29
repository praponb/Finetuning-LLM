from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.max("quantity").alias("max_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)
