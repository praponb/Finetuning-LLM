from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .withColumn("txn_type_group", F.when(F.col("txn_type").isin(["FEE", "CREDIT"]), "Group A").otherwise("Other"))
)
