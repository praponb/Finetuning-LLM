from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("country", ["DE", "UK", "TH", "US"])
    .agg(F.sum("age"))
)
