from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(F.max("amount").alias("max_amount"))
    .orderBy(F.col("max_amount").desc())
)
