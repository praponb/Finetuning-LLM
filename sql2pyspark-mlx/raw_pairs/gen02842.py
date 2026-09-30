from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"))
    .agg(F.count(F.when(F.col("o.status") == "CANCELLED", 1)).alias("cancelled_count"))
    .filter(F.col("cancelled_count") > 2)
    .orderBy("cancelled_count")
)
