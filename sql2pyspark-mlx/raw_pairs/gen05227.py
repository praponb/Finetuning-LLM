from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("country", ["JP", "DE", "SG"])
    .agg(F.avg("age"))
)
