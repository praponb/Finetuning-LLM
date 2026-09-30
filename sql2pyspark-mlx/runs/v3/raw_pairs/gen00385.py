from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.max("age").alias("max_age"))
    .filter(F.col("max_age") >= 63)
    .select("signup_year", "max_age")
    .orderBy(F.col("signup_year").desc())
    .limit(10)
)
