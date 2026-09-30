from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("city", ["Chiang Mai", "Bangkok", "Berlin"])
    .agg(F.max("credit_limit"))
    .orderBy("segment")
)
