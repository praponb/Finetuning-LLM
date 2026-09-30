from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type", "merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant", "account_id").orderBy("session_no", "id")
w5 = Window.partitionBy("txn_type").orderBy("is_credit", "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

base = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-05-20 11:00:00").cast("timestamp")) & (~F.col("merchant").eqNullSafe(F.lit("Shell"))))
    .select(
        "*",
        F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0).alias("is_credit"),
    )
)

flagged = (
    base
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

enriched = (
    sessions_transactions
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("is_credit_bucket", F.ntile(4).over(w5))
)

remaining_keys = (
    enriched
    .select("txn_type")
    .subtract(
        df_transactions
        .filter(~F.col("is_flagged"))
        .select("txn_type")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

base_transactions = (
    kept_rows
    .filter((F.col("txn_time") > F.lit("2023-04-13 22:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .withColumn("seq_no_times_is_credit", (F.col("seq_no") * F.col("is_credit")))
    .withColumn("days_since_txn_time", F.datediff(F.current_date(), "txn_time"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("N/A")))
)

result = (
    base_transactions
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_credit",
        "is_new_burst",
        F.when(F.col("merchant").rlike("^(Starbucks|7-Eleven)$"), "PRIORITY").otherwise("EXTENDED").alias("merchant_zone"),
        F.col("max_transaction_amount"),
    )
    .orderBy(F.col("merchant_zone").desc_nulls_first())
)
