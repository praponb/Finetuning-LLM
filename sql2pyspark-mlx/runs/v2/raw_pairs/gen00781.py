from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("country", ["DE", "TH", "UK"])
    .agg(F.sum("age"))
)
