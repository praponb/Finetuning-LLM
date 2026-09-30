from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") <= 5200)
    .groupBy("country")
    .pivot("segment", ["Corporate", "SMB", "Consumer"])
    .agg(F.count("credit_limit"))
)
