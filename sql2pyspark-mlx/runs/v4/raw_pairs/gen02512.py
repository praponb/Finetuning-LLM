from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.city"), F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(F.avg(F.col("o.shipping_fee")).alias("avg_shipping_fee"))
    .orderBy(F.col("signup_year").desc())
)
