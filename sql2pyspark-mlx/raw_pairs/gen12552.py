from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter(F.col("stores.opened_date") > "2024-05-01")
    .groupBy(F.quarter(F.col("stores.opened_date")).alias("opened_quarter"))
    .agg(
        F.max(F.col("payments.amount")).alias("max_amount"),
        F.sum(F.col("payments.amount")).alias("total_amount"),
    )
)
