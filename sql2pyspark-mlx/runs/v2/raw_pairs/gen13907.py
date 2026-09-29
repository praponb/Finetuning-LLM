from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter((F.col("o.status") == "RETURNED") & (F.col("o.shipping_fee") < 28))
    .select(
        F.col("c.is_active"),
        F.col("c.credit_limit"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("o.discount"),
    )
    .orderBy(F.col("discount").desc(), "channel")
)
