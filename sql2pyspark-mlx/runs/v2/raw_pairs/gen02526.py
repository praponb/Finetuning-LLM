from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .select(
        F.col("o.order_date"),
        F.col("o.store_id"),
        F.col("c.name"),
        F.col("c.email"),
        F.col("c.signup_date"),
        F.col("o.shipping_fee").cast("int").alias("shipping_fee_int"),
    )
    .orderBy(F.col("store_id").desc())
)
