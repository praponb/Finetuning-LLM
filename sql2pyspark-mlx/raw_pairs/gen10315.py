from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.orderBy("balance", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("account_type").orderBy("balance", "id")

with_latest = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.balance",
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

scored = (
    with_latest
    .filter(F.col("transaction_txn_type") == "DEBIT")
    .select(
        "id",
        "balance",
        F.avg("balance").over(w2).alias("balance_avg_3_prior"),
        F.avg("balance").over(w3).alias("balance_avg_2_prior"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id").isin([6, 15, 31, 34])), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time"), F.col("c.amount").alias("transaction_amount"))
)

kept_accounts = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.txn_time")), "left_anti")
)

result = (
    kept_accounts
    .groupBy(
        F.when(F.col("seq_no") >= 7, "HIGH").otherwise("LOW").alias("seq_no_tier"),
        F.when(F.col("balance_avg_2_prior") >= 77076, "PREMIUM").otherwise("STANDARD").alias("balance_avg_2_prior_tier"),
    )
    .agg(
        F.max("transaction_amount").alias("max_transaction_amount"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.percentile_approx("balance", 0.5).alias("balance_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("txn_time").alias("last_seen"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
