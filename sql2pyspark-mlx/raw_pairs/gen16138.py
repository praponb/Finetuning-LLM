from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy(F.col("amount").desc(), "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.min("amount").alias("min_transaction_amount"))

ranked = (
    df_transactions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("min_transaction_amount"),
    )
    .orderBy(F.col("account_id").desc_nulls_first(), "min_transaction_amount")
)
