from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").isNotNull() & F.col("amount").between(2600, 3800))
    .withColumn("merchant_title", F.initcap(F.trim("merchant")))
    .withColumn("is_shell", F.when(F.col("merchant") == "Shell", 1).otherwise(0))
)
