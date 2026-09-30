from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.month("order_date").isin([2, 7, 9]))
    .groupBy("store_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.min("order_date").alias("min_order_date"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("ss.amount_range"),
        F.col("ss.min_order_date"),
    )
    .orderBy(F.col("opened_date").desc())
)
