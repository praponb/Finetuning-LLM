from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.min("last_restock").alias("first_last_restock"),
    )
    .orderBy("first_last_restock")
)
