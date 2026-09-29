from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("city", ["Berlin", "London"])
    .agg(F.max("age"))
)
