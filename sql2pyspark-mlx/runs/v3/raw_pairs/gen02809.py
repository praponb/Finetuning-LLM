from pyspark.sql import functions as F

recent_customers = df_customers.filter(F.col("name").like("Ivy%"))

summary = (
    recent_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
)

result = summary
