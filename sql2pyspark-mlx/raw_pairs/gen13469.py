from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .pivot("country", ["UK", "TH", "DE", "US"])
    .agg(F.avg("credit_limit"))
    .orderBy("is_active")
)
