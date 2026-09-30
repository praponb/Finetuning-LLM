from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("country").isin(["TH", "US", "JP"]) & F.col("age").between(41, 56))
)

summary = (
    base
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(F.round(F.avg("age"), 2).alias("avg_age"))
)

result = (
    summary
    .filter(F.col("avg_age") >= 55)
    .select("signup_month", "avg_age")
    .orderBy("signup_month")
)
