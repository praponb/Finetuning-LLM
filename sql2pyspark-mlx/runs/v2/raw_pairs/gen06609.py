from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("country", ["UK", "SG"])
    .agg(F.count("credit_limit"))
)
