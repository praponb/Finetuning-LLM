from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_new_session",
        F.when(F.col("merchant").rlike("^(Grab|Starbucks)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("merchant_zone"),
        F.col("max_transaction_amount"),
    )
    .orderBy(F.col("max_transaction_amount").desc_nulls_first(), "merchant_zone")
)
