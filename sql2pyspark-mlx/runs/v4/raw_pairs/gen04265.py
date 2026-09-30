from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type")
w2 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy(F.col("balance").asc_nulls_first())
w4 = Window.partitionBy("status").orderBy(F.col("frozen_account_type_avg_balance").desc())

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

kept_keys = (
    base
    .select("id", "merchant")
    .exceptAll(
        df_transactions
        .filter(F.col("amount") >= 2400)
        .select("id", "merchant")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .select("a.*", F.col("p2.status"), F.col("p2.balance"), F.col("p2.account_type"))
)

with_windows = (
    with_dims
    .withColumn("account_type_avg_balance", F.avg("balance").over(w))
    .withColumn("balance_avg_30d", F.avg("balance").over(w2))
    .withColumn("balance_dense_rank", F.dense_rank().over(w3))
)

joined = (
    with_windows
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.balance").alias("account_balance"))
)

joined_transactions = (
    joined
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .select("a.*", F.col("p2.balance").alias("account_balance_2"))
)

result = (
    joined_transactions
    .groupBy(
        "status",
        F.date_trunc("QUARTER", "txn_time").alias("quarter_start"),
        F.when(F.col("account_type_avg_balance") >= 79121, "LARGE").otherwise("SMALL").alias("account_type_avg_balance_tier"),
    )
    .agg(
        F.avg("balance_avg_30d").alias("avg_balance_avg_30d"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("account_type_avg_balance")).otherwise(0)).alias("frozen_account_type_avg_balance"),
        F.round(F.avg("account_balance"), 2).alias("avg_account_balance"),
    )
    .withColumn("frozen_account_type_avg_balance_rank", F.rank().over(w4))
)
