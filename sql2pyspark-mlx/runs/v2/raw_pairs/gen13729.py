from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("city", ["Berlin", "Tokyo", "Chiang Mai"])
    .agg(F.avg("credit_limit"))
)
