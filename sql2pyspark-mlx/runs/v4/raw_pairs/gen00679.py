from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.store_id"),
        F.col("orders.status"),
        F.col("orders.order_date"),
        F.col("customers.country"),
        F.col("customers.name"),
        F.col("customers.signup_date"),
    )
    .orderBy(F.col("store_id").desc(), "id")
    .limit(20)
)
