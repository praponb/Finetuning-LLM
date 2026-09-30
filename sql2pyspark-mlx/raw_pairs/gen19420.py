from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("order_date") > "2024-11-01")
    .groupBy("store_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.round(((100 * F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shipped"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.pct_shipped") > 59)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("t.total_amount"),
        F.col("t.pct_shipped"),
    )
    .orderBy("id")
)
