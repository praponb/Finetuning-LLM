from pyspark.sql import functions as F

base = df_customers.filter(F.col("segment") == "Corporate")

agg = (
    base
    .groupBy("city")
    .agg(F.min("signup_date").alias("first_signup_date"))
)

result = agg.orderBy("first_signup_date")
