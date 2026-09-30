from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("account_type").orderBy("amount", "id")
w3 = Window.partitionBy("account_type").orderBy(F.col("rn").desc(), "id")
w_part = Window.partitionBy("transaction_txn_type").orderBy("amount", "id")
w5 = Window.partitionBy("transaction_txn_type").orderBy(F.col("amount").desc(), "id")

latest_match = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.account_type", F.col("c.amount"))
)

latest = (
    latest_match
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

ranked = (
    latest
    .withColumn("rn_2", F.row_number().over(w3))
    .filter(F.col("rn_2") == 1)
    .select("id", "amount")
)

with_last = (
    ranked
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type").alias("transaction_txn_type"))
)

result = (
    with_last
    .select(
        "id",
        "transaction_txn_type",
        F.ntile(10).over(w_part).alias("amount_bucket"),
        F.row_number().over(w5).alias("row_seq"),
        F.ntile(4).over(w_part).alias("amount_bucket_2"),
    )
)
