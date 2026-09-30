from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isNull())
    .groupBy("segment")
    .pivot("city", ["Berlin", "Bangkok"])
    .agg(F.sum("credit_limit"))
    .orderBy("segment")
)
