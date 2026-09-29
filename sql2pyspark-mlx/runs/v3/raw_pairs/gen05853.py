from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .filter(F.col("orders.shipping_fee") < 13)
    .groupBy(
        F.date_format(F.col("stores.opened_date"), "yyyy-MM").alias("opened_month"),
        F.col("stores.region"),
    )
    .agg(
        F.sum(F.when(F.col("orders.status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
    )
    .filter(F.col("shipped_count") >= 66)
)
