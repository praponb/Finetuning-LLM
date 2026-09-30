from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "TH")
    .groupBy("city")
    .pivot("segment", ["Corporate", "SMB"])
    .agg(F.sum("credit_limit"))
)
