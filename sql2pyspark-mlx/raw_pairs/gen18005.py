from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("txn_time")).orderBy("amount", "id")
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w4 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-08-25").cast("date")) & (F.col("merchant").eqNullSafe(F.lit("Lazada"))))
    .select(
        "*",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_clean"),
    )
)

cleaned = (
    filtered
    .filter(F.col("txn_time") > F.lit("2023-11-01").cast("date"))
    .select(
        "*",
        F.round((F.col("amount_int") / F.col("amount")), 2).alias("amount_int_per_amount"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant_clean",
        "merchant_filled",
        F.last("merchant", ignorenulls=True).over(w).alias("last_known_merchant"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

with_gaps = (
    metrics
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w3).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w4))

with_dims = (
    sessions_transactions
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .filter(F.month(F.col("a.opened_date")).isin([6, 10, 12]))
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.txn_time",
        "s.row_seq",
        "s.is_new_burst",
        F.col("a.opened_date"),
    )
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "LARGE").otherwise("SMALL").alias("is_new_burst_tier"),
        F.when(F.col("row_seq") >= 8, "HIGH").otherwise("LOW").alias("row_seq_tier"),
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
    )
    .agg(
        F.percentile_approx("is_new_burst", [0.25, 0.5, 0.75]).alias("is_new_burst_quartiles"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
    )
)
