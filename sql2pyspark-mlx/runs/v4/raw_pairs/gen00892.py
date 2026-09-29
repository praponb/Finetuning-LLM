from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("amount") <= 1450)
    .groupBy("store_id")
    .agg(
        F.countDistinct("discount").alias("unique_customers"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "inner")
    .filter(F.col("obs.unique_customers") >= 4)
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("obs.unique_customers"),
        F.col("obs.stddev_amount"),
    )
    .orderBy(F.col("unique_customers").desc(), "id")
)
