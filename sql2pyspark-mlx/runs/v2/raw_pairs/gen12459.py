from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") != "SMB") & (F.col("city") == "Chiang Mai"))
    .groupBy("is_active")
    .agg(F.max("credit_limit").alias("max_credit_limit"))
    .orderBy("max_credit_limit")
)
