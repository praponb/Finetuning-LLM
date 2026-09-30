from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("txn_time"), "account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w3 = Window.partitionBy("account_id").orderBy(F.col("amount").asc_nulls_first())
w4 = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

metrics = (
    df_transactions
    .filter(F.col("amount").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("amount").over(w).alias("running_amount"),
        F.lag("amount", 2).over(w2).alias("prev_amount"),
        F.percent_rank().over(w3).alias("amount_percent_rank"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

result = (
    metrics
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "running_amount",
        "prev_amount",
        "amount_percent_rank",
        "seq_no",
        F.col("avg_transaction_amount"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
