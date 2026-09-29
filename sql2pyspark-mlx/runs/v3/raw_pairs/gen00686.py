from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("country", ["SG", "JP"])
    .agg(F.count("age"))
    .orderBy("segment")
)
