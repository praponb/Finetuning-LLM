from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "7-Eleven") & F.col("is_flagged"))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("merchant_upper", F.upper("merchant"))
)
