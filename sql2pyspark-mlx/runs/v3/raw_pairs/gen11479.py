from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("quantity").alias("max_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)
