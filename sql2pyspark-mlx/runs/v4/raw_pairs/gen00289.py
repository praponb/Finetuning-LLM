from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type").orderBy(F.col("txn_time").asc_nulls_first())
w2 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")
w3 = Window.partitionBy("merchant").orderBy(F.col("amount_filled").desc(), "id")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

latest = (
    valid
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    latest
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("amount_filled_bucket", F.ntile(4).over(w3))
)

valid_transactions = (
    enriched
    .filter((F.col("txn_time") > F.lit("2024-01-30").cast("date")) & F.col("merchant").isNotNull())
    .withColumn("amount_filled_bucket_minus_amount_filled", (F.col("amount_filled_bucket") - F.col("amount_filled")))
    .withColumn("clean_txn_type", F.trim("txn_type"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("NONE")))
)

filtered = (
    valid_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("txn_type_merchant", F.concat("txn_type", F.lit(", "), "merchant"))
    .withColumn("amount_filled_bucket_filled", F.coalesce("amount_filled_bucket", F.lit(0)))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("DIRECT")))
)

remaining_keys = (
    filtered
    .select("account_id", "id")
    .subtract(
        df_transactions
        .filter(F.col("txn_time") >= "2024-02-01")
        .select("account_id", "id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_transactions = (
    verified
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "inner")
    .select("e.*", F.col("a.opened_date"))
)

result = (
    enriched_transactions
    .groupBy(
        F.when(F.col("row_seq") >= 9, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
        F.when(F.col("amount_filled_bucket_minus_amount_filled") >= 70, "LARGE").otherwise("SMALL").alias("amount_filled_bucket_minus_amount_filled_tier"),
    )
    .agg(
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
        F.min("txn_time").alias("first_txn_time"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount_filled_bucket_filled")).otherwise(0)).alias("refund_amount_filled_bucket_filled"),
        F.avg(F.when(F.col("txn_type") == "REFUND", F.col("row_seq"))).alias("refund_avg_row_seq"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
    )
    .orderBy(F.col("amount_filled_bucket_minus_amount_filled_tier").desc())
)
