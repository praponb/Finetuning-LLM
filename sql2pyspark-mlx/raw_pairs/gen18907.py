from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["Tokyo", "New York", "London"]) & (F.col("credit_limit") >= 10700))
    .groupBy("city")
    .agg(F.round(F.avg("age"), 2).alias("avg_age"))
    .orderBy(F.col("avg_age").desc(), F.col("city").desc())
    .limit(20)
)
