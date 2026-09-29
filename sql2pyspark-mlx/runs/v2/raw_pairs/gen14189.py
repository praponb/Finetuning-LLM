from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(9, 13) & F.col("amount").between(1700, 2800) & F.col("amount").between(1700, 2000))
    .withColumn("txn_type_upper", F.upper("txn_type"))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .orderBy("id")
    .limit(3)
)
