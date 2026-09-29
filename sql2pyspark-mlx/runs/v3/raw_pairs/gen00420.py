from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .groupBy(
        F.date_format(F.col("stores.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(F.round(F.stddev(F.col("inventory.quantity")), 2).alias("stddev_quantity"))
)
