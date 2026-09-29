from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.amount") >= 1090)
    .groupBy(F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"))
    .agg(F.round(F.avg(F.col("o.amount")), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") >= 55)
)
