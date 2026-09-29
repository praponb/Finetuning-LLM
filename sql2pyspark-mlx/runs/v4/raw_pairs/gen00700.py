from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") != "US")
    .groupBy("segment")
    .pivot("country", ["UK", "US"])
    .agg(F.sum("credit_limit"))
    .orderBy("segment")
)
