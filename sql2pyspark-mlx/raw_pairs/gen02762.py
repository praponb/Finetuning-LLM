from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type")
w2 = Window.partitionBy("txn_type")
w3 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("amount").desc(), "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

metrics = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "is_flagged",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.count("amount").over(w2).alias("txn_type_count_amount"),
        F.sum("amount").over(w3).alias("running_amount"),
        F.ntile(10).over(w4).alias("amount_bucket"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share_2"),
    )
)

result = (
    metrics
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "is_flagged",
        "amount_share",
        "txn_type_count_amount",
        "running_amount",
        F.col("max_transaction_amount"),
    )
    .orderBy(F.col("account_id").desc_nulls_last(), "max_transaction_amount")
)
