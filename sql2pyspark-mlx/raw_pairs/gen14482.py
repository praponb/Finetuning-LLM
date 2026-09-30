from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "CLOSED") & F.col("balance").isNotNull() & (F.col("account_type") != "LOAN"))
    .withColumn("balance_whole", F.floor("balance"))
    .withColumn("balance_band", F.when(F.col("balance") >= 78000, "High").when(F.col("balance") >= 18000, "Medium").otherwise("Low"))
)
