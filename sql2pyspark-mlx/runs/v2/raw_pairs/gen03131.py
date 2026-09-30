from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"), F.col("c.city"))
    .agg(F.sum(F.col("o.amount")).alias("total_amount"))
    .orderBy("city", "signup_month")
    .limit(3)
)
