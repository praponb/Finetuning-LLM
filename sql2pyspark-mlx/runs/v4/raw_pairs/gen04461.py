from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.partitionBy("merchant").orderBy(F.col("txn_time").asc_nulls_first(), "id")
w3 = Window.partitionBy("merchant", "account_id").orderBy("txn_time", "id")
w_part = Window.partitionBy("merchant").orderBy("txn_time", "id")
w5 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

cleaned = (
    deduped
    .filter(F.col("merchant") == "Grab")
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.substring("merchant", 1, 3).alias("merchant_prefix"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled"),
    )
)

deduped_transactions = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    deduped_transactions
    .select(
        "*",
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w_part).alias("seq_no"),
        F.first("merchant", ignorenulls=True).over(w5).alias("first_known_merchant"),
    )
)

remaining_keys = (
    metrics
    .select("account_id")
    .intersect(
        df_transactions
        .filter(~F.col("is_flagged"))
        .select("account_id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("account_id").isin([1, 13, 16]))
    .select(
        "id",
        "account_id",
        "merchant",
        "is_flagged",
        "merchant_prefix",
        "row_seq",
        "first_known_merchant",
        F.coalesce("seq_no", F.lit(0)).alias("seq_no_filled"),
    )
)

base = (
    filtered
    .filter(F.col("row_seq").between(6, 7))
    .select(
        "id",
        "merchant",
        "is_flagged",
        "merchant_prefix",
        "first_known_merchant",
        "seq_no_filled",
        F.when(F.col("merchant").isin(["Lazada", "Amazon"]), "Priority").otherwise("Other").alias("merchant_group"),
    )
)

result = (
    base
    .withColumn("seq_no_filled_tier", F.when(F.col("seq_no_filled") >= 84, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["merchant", "seq_no_filled_tier"], ["merchant"], []],
        "merchant",
        "seq_no_filled_tier",
    )
    .agg(
        F.percentile_approx("seq_no_filled", [0.25, 0.5, 0.75]).alias("seq_no_filled_quartiles"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.round(F.avg("seq_no_filled"), 2).alias("avg_seq_no_filled"),
        F.count_if(F.col("merchant") == "Starbucks").alias("starbucks_events"),
    )
    .filter(F.col("avg_seq_no_filled") > 0)
    .orderBy("merchant")
)
