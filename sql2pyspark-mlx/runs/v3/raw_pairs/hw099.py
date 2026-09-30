from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("country", ["TH", "US", "SG"])
    .agg(F.sum("credit_limit"))
)
