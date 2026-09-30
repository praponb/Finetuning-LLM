from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") <= "2023-04-01") & (F.length("email") >= 12) & (F.length("email") < 12))
    .groupBy("segment", F.year("signup_date").alias("signup_year"))
    .agg(
        F.count("*").alias("num_customers"),
        F.sum(F.when(F.col("city") == "New York", 1).otherwise(0)).alias("new_york_count"),
        (F.max("age") - F.min("age")).alias("age_range"),
    )
)
