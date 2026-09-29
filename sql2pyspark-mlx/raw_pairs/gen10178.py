from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.month(F.col("c.signup_date")).isin([2, 7, 10]))
    .select(F.col("o.amount"), F.col("s.region"), F.col("c.credit_limit"))
    .orderBy("region", F.col("credit_limit").desc())
)
