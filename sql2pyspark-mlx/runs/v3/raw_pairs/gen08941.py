from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.max("age").alias("max_age"))
    .filter(F.col("max_age") > 38)
)
