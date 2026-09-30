from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("o.shipping_fee") > 38)
    .select(
        F.col("o.channel"),
        F.col("o.store_id"),
        F.col("c.name"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
    )
    .orderBy("store_id")
)
