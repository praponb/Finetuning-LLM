from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["London", "Chiang Mai"]) | (F.length("name") < 7))
    .rollup("city", F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.sum(F.when(F.col("country") == "TH", 1).otherwise(0)).alias("th_count"))
    .orderBy("city", "signup_month")
)
