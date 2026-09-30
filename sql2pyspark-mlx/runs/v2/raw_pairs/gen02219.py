from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "outer")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter(F.col("stores.sqft") > 4799)
    .groupBy(F.month(F.col("stores.opened_date")).alias("opened_month"))
    .agg(
        F.avg(F.col("payments.amount")).alias("avg_amount"),
        F.count(F.col("payments.amount")).alias("amount_count"),
    )
    .filter(F.col("amount_count") >= 3)
)
