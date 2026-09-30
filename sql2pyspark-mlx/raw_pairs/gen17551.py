from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("merchant", "account_id").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w_running = Window.partitionBy("merchant").orderBy("last7_avg_amount", "id")
w3 = Window.partitionBy("merchant").orderBy(F.col("txn_time").asc_nulls_last(), "id")
w4 = Window.partitionBy(F.month("txn_time")).orderBy(F.col("row_seq").desc(), "id")
w_hist = Window.orderBy("row_seq", "id")

scored = (
    df_transactions
    .withColumn("last7_sum_amount", F.sum("amount").over(w_part))
    .withColumn("last7_max_amount", F.max("amount").over(w_part))
    .withColumn("last7_avg_amount", F.avg("amount").over(w_part))
)

base = (
    scored
    .filter((F.col("txn_time") > F.lit("2023-11-10 03:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "last7_sum_amount",
        "last7_max_amount",
        "last7_avg_amount",
        F.lower("merchant").alias("merchant_lower"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_filled"),
    )
)

metrics = (
    base
    .withColumn("next_amount", F.lead("amount").over(w_running))
    .withColumn("prev_amount", F.lag("amount").over(w_running))
    .withColumn("prev_last7_avg_amount", F.lag("last7_avg_amount").over(w_running))
    .withColumn("row_seq", F.row_number().over(w_running))
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "amount", "txn_time", "next_amount", "row_seq")
)

enriched = (
    ranked
    .select(
        "id",
        "amount",
        "next_amount",
        "row_seq",
        F.ntile(10).over(w4).alias("row_seq_bucket"),
        F.lag("amount", 1).over(w_hist).alias("prev_amount"),
    )
)

matched_keys = (
    enriched
    .select("id")
    .intersect(
        df_transactions
        .filter(F.col("txn_type") != "FEE")
        .select("id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys = (
    kept_rows
    .select("id")
    .subtract(
        df_transactions
        .filter(F.hour("txn_time").between(12, 17))
        .select("id")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "amount", "next_amount", "row_seq_bucket")
    .orderBy(F.col("amount").desc_nulls_first(), "next_amount")
)
