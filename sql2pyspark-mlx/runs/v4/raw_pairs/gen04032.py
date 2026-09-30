from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city", "segment")
    .pivot("country", ["US", "SG"])
    .agg(F.max("credit_limit").alias("amount"))
)
