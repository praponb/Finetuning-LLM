from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("country", ["TH", "JP", "UK"])
    .agg(F.count("credit_limit"))
)
