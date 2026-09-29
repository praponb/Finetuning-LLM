from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy("amount", "id")
w2 = Window.partitionBy("account_id", "merchant").orderBy("txn_time", "id")
w3 = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc())
w4 = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc(), "id")
w5 = Window.partitionBy(F.year("txn_time"), "merchant").orderBy("rank_in_group", "id")
w6 = Window.partitionBy("merchant", "status").orderBy(F.col("rn_2").desc(), "id")

latest = (
    df_transactions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "account_id", "txn_time", "merchant")
)

deduped = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

deduped_transactions = (
    ranked
    .withColumn("rn_2", F.row_number().over(w4))
    .filter(F.col("rn_2") == 1)
)

joined = (
    deduped_transactions
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select("s.*", F.col("a.status"))
)

result = (
    joined
    .select(
        "id",
        "txn_time",
        "rn",
        "rank_in_group",
        "rn_2",
        "status",
        F.row_number().over(w5).alias("row_seq"),
        F.row_number().over(w6).alias("row_seq_2"),
    )
)
