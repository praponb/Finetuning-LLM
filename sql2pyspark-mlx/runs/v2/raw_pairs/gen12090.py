from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .filter(F.year(F.col("stores.opened_date")) >= 2023)
    .groupBy(F.col("stores.region"))
    .agg(
        F.count(F.col("inventory.quantity")).alias("quantity_count"),
        F.round(F.avg(F.col("inventory.quantity")), 2).alias("avg_quantity"),
    )
)
