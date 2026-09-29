from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("country", ["UK", "JP", "DE", "SG"])
    .agg(F.avg("age"))
)
