from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"), "country")
    .agg(F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"))
    .filter(F.col("uk_count") < 47)
    .select("signup_month", "country", "uk_count")
    .orderBy("country")
)
