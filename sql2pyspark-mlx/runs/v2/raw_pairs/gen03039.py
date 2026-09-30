from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Consumer", "SMB", "Corporate"]) & F.col("segment").isin(["Corporate", "SMB", "Consumer"]))
    .groupBy("segment")
    .agg(F.min("signup_date").alias("min_signup_date"))
    .orderBy("segment")
    .limit(20)
)
