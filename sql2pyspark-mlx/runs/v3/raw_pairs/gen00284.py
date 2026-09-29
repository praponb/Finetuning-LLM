from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", F.year("signup_date").alias("signup_year"))
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        F.round(((100 * F.sum(F.when(F.col("country") == "UK", 1).otherwise(0))) / F.count("*")), 1).alias("pct_uk"),
        F.count("*").alias("num_customers"),
    )
    .filter(F.col("num_customers") >= 1)
    .drop("num_customers")
)
