from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["Chiang Mai", "New York", "Tokyo"])
    .agg(F.max("credit_limit"))
)
