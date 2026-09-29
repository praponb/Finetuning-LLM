from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("country", ["TH", "SG", "DE"])
    .agg(F.count("age"))
)
