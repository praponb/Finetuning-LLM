from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("c.signup_date"),
        F.concat_ws(",", F.col("c.name"), F.col("c.city")).alias("name_city_key"),
    )
)
