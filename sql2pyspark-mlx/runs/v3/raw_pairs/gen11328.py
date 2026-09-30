from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    df_stores.alias("stores")
    .join(store_totals.alias("store_totals"), F.col("stores.id") == F.col("store_totals.store_id"), "inner")
    .select(
        F.col("stores.id"),
        F.col("stores.region"),
        F.col("stores.opened_date"),
        F.col("store_totals.stddev_quantity"),
    )
    .orderBy(F.col("opened_date").desc(), F.col("stddev_quantity").desc(), "id")
    .limit(50)
)
