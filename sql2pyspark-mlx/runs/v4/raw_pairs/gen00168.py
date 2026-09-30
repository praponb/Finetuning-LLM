from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .withColumn("amount_tier", F.when(F.col("amount") >= 3400, "High").when(F.col("amount") >= 1200, "Medium").otherwise("Low"))
    .withColumn("merchant_title", F.initcap(F.trim("merchant")))
    .orderBy("txn_time", "id")
)
