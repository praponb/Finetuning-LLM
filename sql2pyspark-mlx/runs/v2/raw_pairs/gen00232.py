from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("city", ["Bangkok", "New York"])
    .agg(F.avg("credit_limit"))
)
