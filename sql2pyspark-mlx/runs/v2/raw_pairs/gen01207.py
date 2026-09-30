from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("country", ["UK", "SG", "JP"])
    .agg(F.max("age"))
    .orderBy("city")
)
