from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.min("amount").alias("min_transaction_amount"))

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    islands
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "is_flagged",
        "session_no",
        F.when(F.col("txn_type").rlike("^(DEBIT|REFUND)$"), "GROUP_A").when(F.col("txn_type").rlike("^(CREDIT|FEE)$"), "CORE").otherwise("GROUP_B").alias("txn_type_zone"),
        F.col("min_transaction_amount"),
    )
    .orderBy(F.col("txn_type_zone").desc_nulls_last())
)
