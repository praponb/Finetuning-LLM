from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(7, 12) & ~F.col("txn_type").isin(["FEE", "REFUND"]))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .orderBy("txn_time", "id")
    .limit(20)
)
