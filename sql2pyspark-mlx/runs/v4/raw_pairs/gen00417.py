from pyspark.sql import functions as F

result = (
    df_customers
    .cube("segment", F.date_trunc("YEAR", "signup_date").alias("year_start"))
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.countDistinct("city").alias("distinct_cities"),
        F.count("*").alias("num_rows"),
    )
)
