from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("country", ["TH", "DE", "JP"])
    .agg(F.avg("age"))
    .orderBy("city")
)
