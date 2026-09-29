from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id")
w2 = Window.partitionBy("account_id").orderBy("amount", "id")
w3 = Window.partitionBy("merchant")
w4 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("merchant").orderBy("txn_time", "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

with_windows = (
    df_transactions
    .select(
        "*",
        F.count("amount").over(w).alias("merchant_count_amount"),
        F.lag("amount", 1).over(w2).alias("prev_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.avg("amount").over(w4).alias("amount_avg_6_prior"),
        F.lead("amount", 1).over(w5).alias("next_amount"),
    )
)

result = (
    with_windows
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "merchant_count_amount",
        "amount_avg_6_prior",
        "next_amount",
        F.col("sum_transaction_amount"),
    )
)
