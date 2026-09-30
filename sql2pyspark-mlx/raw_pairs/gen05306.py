from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") <= 15200)
    .groupBy("segment")
    .pivot("country", ["US", "DE"])
    .agg(F.sum("credit_limit"))
    .orderBy("segment")
)
