from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("signup_date", F.lit("customer").alias("source"))
    .union(df_orders.select("order_date", F.lit("order").alias("source")))
    .orderBy(F.col("signup_date").desc())
)
