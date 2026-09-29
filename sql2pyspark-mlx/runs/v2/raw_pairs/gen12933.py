from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(2300, 4000) & F.col("is_flagged"))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)
