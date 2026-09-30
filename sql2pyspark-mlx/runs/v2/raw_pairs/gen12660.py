from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1800, 3900) & (F.col("account_id") == 38))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("amount_int", F.col("amount").cast("int"))
    .orderBy(F.col("account_id").desc())
)
