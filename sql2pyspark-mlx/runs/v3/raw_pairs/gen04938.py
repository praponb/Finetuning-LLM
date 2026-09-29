from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.round(((100 * F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shipped"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("O"), F.col("s.id") == F.col("O.store_id"), "inner")
    .filter(F.col("O.stddev_discount") > 58)
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("O.stddev_discount"),
        F.col("O.pct_shipped"),
    )
    .orderBy("id")
)
