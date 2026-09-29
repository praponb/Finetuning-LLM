from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("city", ["Singapore", "New York", "Berlin", "Chiang Mai"])
    .agg(F.max("credit_limit"))
)
