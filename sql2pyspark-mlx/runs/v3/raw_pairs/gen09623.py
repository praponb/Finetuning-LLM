from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "right")
    .filter(F.col("stores.store_name").like("%Downtown%"))
    .groupBy(
        F.col("stores.region"),
        F.date_trunc("MONTH", F.col("stores.opened_date")).alias("opened_month"),
    )
    .agg(F.round(F.stddev(F.col("inventory.quantity")), 2).alias("stddev_quantity"))
    .orderBy(F.col("stddev_quantity").desc())
)
