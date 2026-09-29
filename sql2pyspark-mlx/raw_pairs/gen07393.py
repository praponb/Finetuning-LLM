from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("segment", ["Consumer", "SMB"])
    .agg(F.avg("credit_limit"))
    .select("city", F.col("Consumer").alias("consumer"), F.col("SMB").alias("smb"))
)
