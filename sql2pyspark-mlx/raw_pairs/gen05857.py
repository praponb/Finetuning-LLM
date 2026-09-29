from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "account_status").orderBy("balance", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("balance", "id")

cleaned = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "FEE"]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.status"))
)

base = (
    enriched
    .filter((F.col("txn_time") > F.to_date(F.lit("2023-07-19"))) & (F.col("merchant") == "Amazon"))
    .withColumn("clean_merchant", F.trim("merchant"))
    .withColumn("merchant_txn_type", F.concat("merchant", F.lit(" "), "txn_type"))
)

enriched_transactions = (
    base
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.account_id",
        "e.amount",
        "e.is_flagged_flag",
        "e.clean_merchant",
        F.col("p.balance"),
        F.col("p.opened_date"),
        F.col("p.status").alias("account_status"),
    )
)

enriched_transactions_2 = (
    enriched_transactions
    .filter(F.col("account_id").isin([2, 18, 19, 30]))
    .select(
        "*",
        F.avg("amount").over(w).alias("last4_avg_amount"),
        F.count("amount").over(w).alias("last4_count_amount"),
        F.min("amount").over(w).alias("last4_min_amount"),
        F.lag("balance", 1, 0).over(w2).alias("prev_balance"),
    )
)

joined = (
    enriched_transactions_2
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "inner")
    .select("s.*", F.col("a.balance").alias("account_balance"), F.col("a.account_type"))
)

enriched_transactions_3 = (
    joined
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .select(
        "a.*",
        F.col("d.status"),
        F.col("d.opened_date").alias("account_opened_date"),
    )
)

result = (
    enriched_transactions_3
    .cube(F.date_trunc("quarter", "opened_date").alias("quarter_start"))
    .agg(
        F.percentile_approx("account_balance", 0.5).alias("account_balance_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .orderBy(F.col("avg_balance").desc())
)
