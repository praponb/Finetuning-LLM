from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

deduped = (
    df_transactions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "is_flagged",
        F.when(F.col("txn_type").rlike("^(CREDIT|FEE)$"), "EXTENDED").when(F.col("txn_type").rlike("^(DEBIT|REFUND)$"), "OTHER_ZONE").otherwise("CORE").alias("txn_type_zone"),
        F.col("max_transaction_amount"),
    )
    .orderBy(F.col("amount").asc_nulls_last(), F.col("account_id").desc())
)
