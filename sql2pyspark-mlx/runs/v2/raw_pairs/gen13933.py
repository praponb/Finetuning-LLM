from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("country", ["DE", "JP", "TH"])
    .agg(F.sum("age"))
    .orderBy("segment")
)
