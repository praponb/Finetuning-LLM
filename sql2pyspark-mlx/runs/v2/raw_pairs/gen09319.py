from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("country", ["JP", "TH"])
    .agg(F.max("credit_limit"))
    .orderBy("segment")
)
