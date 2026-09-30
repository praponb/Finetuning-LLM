from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("c.age"),
        F.date_sub(F.col("c.signup_date"), 78).alias("signup_date_minus_78d"),
    )
    .orderBy(F.col("channel").desc())
)
