from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .groupBy(F.col("stores.region"))
    .agg(
        F.min(F.col("payments.amount")).alias("min_amount"),
        F.count("*").alias("cnt"),
    )
    .orderBy("min_amount")
)
