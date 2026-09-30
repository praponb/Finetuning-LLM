from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("txn_type")
w4 = Window.partitionBy("txn_type").orderBy("amount_max_30d", "id")
w5 = Window.partitionBy("txn_type").orderBy("txn_time_weekofyear_count_7d", "id")
w6 = Window.partitionBy("txn_type").orderBy("next_txn_time_weekofyear_count_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("txn_time_weekofyear_count_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("txn_type").orderBy("txn_type_count_txn_time_weekofyear_count_7d", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.partitionBy("txn_type").orderBy("txn_type_count_txn_time_weekofyear_count_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("amount_max_30d", "id")

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.weekofyear("txn_time").alias("txn_time_weekofyear"),
    )
)

valid = (
    filtered
    .filter(F.col("txn_time") >= F.to_timestamp(F.lit("2023-04-30 16:00:00")))
    .withColumn("amount_band", F.when(F.col("amount") >= 4500, "High").when(F.col("amount") >= 1000, "Medium").otherwise("Low"))
)

cleaned = (
    valid
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") >= F.lit("2023-10-22 08:00:00").cast("timestamp")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "txn_time_weekofyear",
        "amount_band",
        F.substring("merchant", 1, 3).alias("merchant_prefix"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant_prefix",
        F.max("amount").over(w).alias("amount_max_30d"),
        F.count("txn_time_weekofyear").over(w2).alias("txn_time_weekofyear_count_7d"),
    )
)

cleaned_transactions = (
    metrics
    .filter((F.col("txn_time") >= F.lit("2023-10-13 00:00:00").cast("timestamp")) & (F.col("txn_time_weekofyear_count_7d") > 10))
    .select(
        "id",
        "txn_type",
        "amount_max_30d",
        "txn_time_weekofyear_count_7d",
        F.date_format("txn_time", "yyyy-MM-dd").alias("txn_time_label"),
    )
)

enriched = (
    cleaned_transactions
    .filter(F.col("amount_max_30d") != 2360)
    .select(
        "id",
        "txn_type",
        "amount_max_30d",
        "txn_time_weekofyear_count_7d",
        "txn_time_label",
        F.count("txn_time_weekofyear_count_7d").over(w3).alias("txn_type_count_txn_time_weekofyear_count_7d"),
        F.ntile(3).over(w4).alias("amount_max_30d_bucket"),
        F.lead("txn_time_weekofyear_count_7d", 1, 0).over(w5).alias("next_txn_time_weekofyear_count_7d"),
    )
)

metrics_transactions = (
    enriched
    .select(
        "id",
        "txn_type",
        "amount_max_30d",
        "amount_max_30d_bucket",
        F.sum("txn_type_count_txn_time_weekofyear_count_7d").over(w6).alias("cumulative_txn_type_count_txn_time_weekofyear_count_7d"),
        F.sum("txn_time_weekofyear_count_7d").over(w7).alias("cumulative_txn_time_weekofyear_count_7d"),
        F.avg("txn_type_count_txn_time_weekofyear_count_7d").over(w_running).alias("txn_type_count_txn_time_weekofyear_count_7d_avg_5"),
        F.last("txn_type_count_txn_time_weekofyear_count_7d", ignorenulls=True).over(w9).alias("last_known_txn_type_count_txn_time_weekofyear_count_7d"),
        F.row_number().over(w10).alias("seq_no"),
    )
)

result = (
    metrics_transactions
    .select(
        "id",
        "txn_type",
        "amount_max_30d_bucket",
        "cumulative_txn_type_count_txn_time_weekofyear_count_7d",
        "cumulative_txn_time_weekofyear_count_7d",
        "last_known_txn_type_count_txn_time_weekofyear_count_7d",
        "seq_no",
        F.when(F.col("txn_type").rlike("^(REFUND|FEE)$"), "GROUP_B").when(F.col("txn_type").rlike("^(DEBIT|CREDIT)$"), "PRIORITY").otherwise("CORE").alias("txn_type_zone"),
    )
    .orderBy("cumulative_txn_type_count_txn_time_weekofyear_count_7d", "seq_no")
)
