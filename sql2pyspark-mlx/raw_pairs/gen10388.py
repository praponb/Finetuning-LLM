from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("country", ["TH", "JP"])
    .agg(F.sum("age"))
    .orderBy("city")
)
