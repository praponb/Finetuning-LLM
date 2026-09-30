from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(F.countDistinct(F.col("s.customer_id")).alias("unique_customers"))
    .orderBy("unique_customers")
)
