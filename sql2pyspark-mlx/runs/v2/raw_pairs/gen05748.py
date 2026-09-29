from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("discount") < 0.25)
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_count"),
        F.min("order_date").alias("min_order_date"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("t.total_count"),
        F.col("t.min_order_date"),
    )
    .orderBy("sqft")
)
