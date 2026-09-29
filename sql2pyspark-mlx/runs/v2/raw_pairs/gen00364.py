from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.year(F.col("c.signup_date")) == 2024)
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.channel"),
        F.col("c.country"),
        F.col("s.sqft"),
        F.col("s.city"),
    )
    .orderBy(F.col("id").desc())
)
