from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc(), "id")

latest = (
    df_transactions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "account_id", "txn_type", "txn_time")
)

filtered = (
    latest
    .filter(F.col("txn_time") < "2023-11-01")
    .withColumn("txn_type_prefix", F.substring("txn_type", 1, 3))
)

latest_transactions = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "account_id")
)

with_dims = (
    latest_transactions
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .select("src.id", "src.account_id", F.col("d.account_type"))
)

result = (
    with_dims
    .cube("account_type", "account_id")
    .agg(
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.size(F.collect_set("account_type")).alias("num_account_types_2"),
        F.size(F.collect_set("account_type")).alias("num_account_types_3"),
    )
)
