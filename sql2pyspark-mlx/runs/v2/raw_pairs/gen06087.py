from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .pivot("country", ["SG", "UK", "TH"])
    .agg(F.avg("age"))
)
