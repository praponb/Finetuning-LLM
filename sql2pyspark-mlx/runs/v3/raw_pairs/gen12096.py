from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("city", ["Berlin", "London", "Singapore"])
    .agg(F.avg("credit_limit"))
    .orderBy("segment")
)
