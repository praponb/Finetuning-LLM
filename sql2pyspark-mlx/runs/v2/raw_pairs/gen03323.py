from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "outer")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.month(F.col("customers.signup_date")).isin([4, 5, 6]))
    .groupBy(
        F.date_format(F.col("stores.opened_date"), "yyyy-MM").alias("opened_month"),
        F.col("stores.city"),
    )
    .agg(
        F.count(F.when(F.col("orders.channel") == "mobile", 1)).alias("mobile_count"),
        F.min(F.col("orders.order_date")).alias("first_order_date"),
    )
)
