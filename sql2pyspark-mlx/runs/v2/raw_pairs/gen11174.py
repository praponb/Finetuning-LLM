from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([15, 18]) & F.col("customer_id").isin([8, 15, 35, 36]) & F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
    .withColumn("balance_level", F.when(F.col("balance") >= 80000, "High").when(F.col("balance") >= 25000, "Medium").otherwise("Low"))
    .withColumn("balance_adjusted", F.round((F.col("balance") * 1.15), 2))
    .orderBy("customer_id")
)
