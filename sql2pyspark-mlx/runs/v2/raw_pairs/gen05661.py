from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", F.year("signup_date").alias("signup_year"))
    .agg(F.avg("credit_limit").alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") < 15500)
    .select("segment", "signup_year", "avg_credit_limit")
    .orderBy("signup_year", F.col("segment").desc())
)
