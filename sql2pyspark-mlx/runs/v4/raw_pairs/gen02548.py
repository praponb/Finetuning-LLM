from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") != "Consumer")
    .groupBy("country")
    .pivot("segment", ["Corporate", "SMB"])
    .agg(F.max("age"))
)
