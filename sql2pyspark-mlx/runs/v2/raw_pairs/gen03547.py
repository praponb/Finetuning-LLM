from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "US")
    .groupBy("city", F.year("signup_date").alias("signup_year"))
    .agg(
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .filter(F.col("credit_limit_range") >= 53)
    .select("city", "signup_year", "credit_limit_range", "country_list")
    .orderBy("city", F.col("signup_year").desc())
)
