from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.min("amount").alias("min_transaction_amount"))

with_gaps = (
    df_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_new_session",
        F.when(F.col("txn_type").rlike("^(FEE|REFUND)$"), "EXTENDED").when(F.col("txn_type").rlike("^(DEBIT|CREDIT)$"), "CORE").otherwise("PRIORITY").alias("txn_type_zone"),
        F.col("min_transaction_amount"),
    )
    .orderBy("txn_type", "txn_type_zone")
)
