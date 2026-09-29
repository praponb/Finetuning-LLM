from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(
        F.max("last_restock").alias("last_last_restock"),
        F.max("quantity").alias("max_quantity"),
    )
)
