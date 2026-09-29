from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("name") > 5)
    .groupBy("country", F.year("signup_date").alias("signup_year"))
    .agg(F.count("age").alias("age_count"))
    .orderBy("age_count", "country")
)
