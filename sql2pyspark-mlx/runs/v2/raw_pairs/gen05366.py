from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("city", ["New York", "Berlin", "London"])
    .agg(F.max("age"))
    .orderBy("segment")
)
