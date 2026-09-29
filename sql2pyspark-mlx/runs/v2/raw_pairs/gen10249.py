from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["London", "Berlin", "Singapore", "Tokyo"])
    .agg(F.max("credit_limit"))
    .orderBy("country")
)
