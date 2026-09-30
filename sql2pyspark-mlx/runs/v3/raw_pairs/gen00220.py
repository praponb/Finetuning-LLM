from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.city"), F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(F.min(F.col("o.amount")).alias("min_amount"))
    .orderBy("signup_quarter", "city")
    .limit(20)
)
