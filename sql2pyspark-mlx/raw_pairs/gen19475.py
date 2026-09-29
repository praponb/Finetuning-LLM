from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_new_session",
        "session_id",
        F.when(F.col("txn_type").rlike("^(REFUND|DEBIT)$"), "OTHER_ZONE").when(F.col("txn_type").rlike("^(CREDIT|FEE)$"), "GROUP_B").otherwise("GROUP_A").alias("txn_type_zone"),
        F.col("sum_transaction_amount"),
    )
    .orderBy(F.col("session_id").desc())
)
