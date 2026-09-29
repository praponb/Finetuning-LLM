from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("amount")).otherwise(0)).alias("cancelled_amount"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("O"), F.col("s.id") == F.col("O.store_id"), "left")
    .select(F.col("s.sqft"), F.col("O.cancelled_amount"))
)
