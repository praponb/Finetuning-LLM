from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", F.year("signup_date").alias("signup_year"))
    .agg(
        F.count("credit_limit").alias("credit_limit_count"),
        F.sum(F.when(F.col("country") == "TH", 1).otherwise(0)).alias("th_count"),
    )
)
