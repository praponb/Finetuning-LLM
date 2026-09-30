from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").isNull())
    .select("id", "age", "signup_date", F.ceil("age").alias("age_whole"))
    .orderBy("age")
)
