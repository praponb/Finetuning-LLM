from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["TH", "SG", "US"]))
    .groupBy("city")
    .pivot("country", ["TH", "DE", "UK"])
    .agg(F.max("credit_limit"))
    .orderBy("city")
)
