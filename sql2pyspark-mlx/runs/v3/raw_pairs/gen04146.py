from pyspark.sql import functions as F

store_totals = (
    df_orders
    .groupBy("store_id")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    df_stores.alias("stores")
    .join(store_totals.alias("store_totals"), F.col("stores.id") == F.col("store_totals.store_id"), "inner")
    .filter(F.col("store_totals.stddev_amount") > 22)
    .select(
        F.col("stores.id"),
        F.col("stores.store_name"),
        F.col("stores.region"),
        F.col("store_totals.stddev_amount"),
    )
    .orderBy(F.col("stddev_amount").desc(), "id")
    .limit(3)
)
