from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.alias("orders"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("orders.shipping_fee").between(14, 18))
    .select(
        F.col("customers.age"),
        F.col("orders.order_date"),
        F.round(F.col("orders.discount"), 1).alias("discount_rounded"),
    )
    .orderBy(F.col("discount_rounded").desc())
)
