from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("transaction_merchant").orderBy(F.col("balance_int").desc(), "id")
w3 = Window.partitionBy("transaction_merchant").orderBy("balance_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("transaction_merchant").orderBy("transaction_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("balance_int", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("transaction_merchant").orderBy("running_transaction_amount", "id")
w7 = Window.partitionBy("transaction_merchant").orderBy("running_transaction_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("transaction_merchant").orderBy("transaction_amount_avg_3", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_accounts
    .filter(F.col("balance").between(49000, 79000))
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        F.col("balance").cast("int").alias("balance_int"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.status",
        "s.balance_int",
        F.col("c.merchant").alias("transaction_merchant"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

latest = (
    with_latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    latest
    .select(
        "id",
        "balance_int",
        "transaction_merchant",
        F.row_number().over(w2).alias("seq_no"),
        F.sum("transaction_amount").over(w3).alias("running_transaction_amount"),
        F.sum("transaction_amount").over(w4).alias("cumulative_transaction_amount"),
        F.avg("transaction_amount").over(w5).alias("transaction_amount_avg_3"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "balance_int",
        "transaction_merchant",
        "cumulative_transaction_amount",
        F.sum("running_transaction_amount").over(w7).alias("running_sum_running_transaction_amount"),
        F.max("running_transaction_amount").over(w7).alias("running_max_running_transaction_amount"),
        F.avg("running_transaction_amount").over(w7).alias("running_avg_running_transaction_amount"),
        F.avg("transaction_amount_avg_3").over(w8).alias("transaction_amount_avg_3_avg_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "cumulative_transaction_amount",
        "running_sum_running_transaction_amount",
        "running_max_running_transaction_amount",
        "running_avg_running_transaction_amount",
        "transaction_amount_avg_3_avg_2",
    )
)
