from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc_nulls_last(), "id")
w_running = Window.partitionBy("merchant").orderBy("is_flagged_flag", "id")

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & ~F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.concat("merchant", F.lit(" "), "txn_type").alias("merchant_txn_type"),
    )
)

enriched = (
    filtered
    .filter(F.col("is_flagged"))
    .withColumn("last_known_merchant", F.last("merchant", ignorenulls=True).over(w))
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "txn_type",
        "merchant",
        "is_flagged",
        "merchant_txn_type",
        "last_known_merchant",
    )
)

valid = (
    latest
    .filter(F.col("merchant").isNotNull())
    .select("*", F.coalesce("merchant", F.lit("Unknown")).alias("merchant_filled"))
)

cleaned = (
    valid
    .filter(F.col("merchant").isNotNull() & F.col("txn_type").isin(["REFUND", "FEE"]))
    .select(
        "id",
        "merchant",
        "is_flagged",
        "merchant_txn_type",
        "merchant_filled",
        F.upper("merchant").alias("merchant_upper"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

valid_transactions = (
    cleaned
    .filter(F.col("merchant") != "Amazon")
    .select(
        "*",
        F.lower("merchant").alias("merchant_lower"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)

metrics = (
    valid_transactions
    .withColumn("prev_is_flagged_flag", F.lag("is_flagged_flag").over(w_running))
    .withColumn("next_is_flagged_flag", F.lead("is_flagged_flag").over(w_running))
    .withColumn("seq_no", F.row_number().over(w_running))
)

result = (
    metrics
    .select(
        "id",
        "is_flagged",
        "merchant_txn_type",
        "resolved_merchant",
        "is_flagged_flag",
        "prev_is_flagged_flag",
        "next_is_flagged_flag",
        "seq_no",
    )
    .orderBy(F.col("is_flagged_flag").desc_nulls_last(), "prev_is_flagged_flag")
)
