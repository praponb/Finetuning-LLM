from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("amount_max_30d", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("txn_type", "merchant").orderBy("last3_avg_amount", "id")
w5 = Window.orderBy("last3_max_amount", "id")
w_ordered = Window.partitionBy("txn_type").orderBy("last7_min_is_debit", "id")

cleaned = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-09-09 10:00:00").cast("timestamp"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.upper("txn_type").alias("txn_type_upper"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "txn_type",
        "merchant",
        "txn_type_upper",
        F.sum("amount").over(w_running).alias("last3_sum_amount"),
        F.avg("amount").over(w_running).alias("last3_avg_amount"),
        F.max("amount").over(w_running).alias("last3_max_amount"),
        F.max("amount").over(w2).alias("amount_max_30d"),
    )
)

filtered = (
    metrics
    .filter(F.col("merchant").isNotNull() & (F.col("merchant") == "Starbucks"))
    .select(
        "id",
        "txn_type",
        "merchant",
        "last3_sum_amount",
        "last3_avg_amount",
        "last3_max_amount",
        "amount_max_30d",
        F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0).alias("is_debit"),
        F.trim("merchant").alias("clean_merchant"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled"),
    )
)

enriched = (
    filtered
    .withColumn("last7_sum_is_debit", F.sum("is_debit").over(w3))
    .withColumn("last7_max_is_debit", F.max("is_debit").over(w3))
    .withColumn("last7_min_is_debit", F.min("is_debit").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

with_windows = (
    enriched
    .filter(F.col("merchant").isin(["Grab", "7-Eleven", "Lazada"]))
    .select(
        "*",
        F.lead("last3_sum_amount", 2).over(w5).alias("next_last3_sum_amount"),
        F.ntile(10).over(w_ordered).alias("last7_min_is_debit_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("last3_max_amount") >= 3100, "LARGE").otherwise("SMALL").alias("last3_max_amount_tier"),
        F.when(F.col("last3_avg_amount") >= 3600, "LARGE").otherwise("SMALL").alias("last3_avg_amount_tier"),
    )
    .agg(
        F.round(F.sum("amount_max_30d") / F.nullif(F.count(F.when(F.col("merchant") == "Grab", 1)), F.lit(0)), 2).alias("amount_max_30d_per_grab"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.min("last3_max_amount").alias("min_last3_max_amount"),
    )
)
