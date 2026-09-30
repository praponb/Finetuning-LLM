from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id")
w2 = Window.orderBy("txn_time", "id").rowsBetween(-6, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.min("amount").alias("min_transaction_amount"))

scored = (
    df_transactions
    .select(
        "*",
        F.lead("amount", 1).over(w).alias("next_amount"),
        F.avg("amount").over(w2).alias("amount_avg_6"),
    )
)

result = (
    scored
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged",
        "next_amount",
        "amount_avg_6",
        F.col("min_transaction_amount"),
    )
    .orderBy(F.col("amount").asc_nulls_first())
)
