from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("city", ["London", "Bangkok", "Berlin"])
    .agg(F.avg("age"))
    .orderBy("segment")
)
