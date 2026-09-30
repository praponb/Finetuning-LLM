from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"), "segment")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") < 8)
    .select("signup_month", "segment", "total_count")
)
