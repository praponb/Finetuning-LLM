from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 4)
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("quantity").alias("max_quantity"),
    )
)
