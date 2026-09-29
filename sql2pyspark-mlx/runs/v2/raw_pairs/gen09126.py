from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.count("age").alias("age_count"))
    .filter(F.col("age_count") < 2)
    .orderBy(F.col("age_count").desc(), "signup_year")
    .limit(5)
)
