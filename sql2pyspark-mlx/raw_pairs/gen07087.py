from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "right")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("o.shipping_fee") > 17)
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("s.sqft"),
        F.col("c.city"),
        F.col("c.signup_date"),
        F.concat_ws(",", F.col("s.city"), F.col("s.region")).alias("city_region_key"),
    )
    .orderBy(F.col("city").desc())
)
