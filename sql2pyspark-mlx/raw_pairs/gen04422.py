from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("merchant", "account_id").orderBy("txn_time_hour", "id")
w2 = Window.partitionBy("merchant", "account_id").orderBy(F.col("is_flagged_flag").desc_nulls_first())
w3 = Window.partitionBy("account_id")
w4 = Window.orderBy(F.col("prev_is_flagged_flag").asc_nulls_first())
w5 = Window.partitionBy("account_id", "merchant").orderBy("row_seq")
w6 = Window.partitionBy("merchant")

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "merchant",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.hour("txn_time").alias("txn_time_hour"),
    )
)

enriched = (
    base
    .filter(F.col("is_flagged"))
    .select(
        "*",
        F.lag("is_flagged_flag").over(w_ordered).alias("prev_is_flagged_flag"),
        F.lead("txn_time_hour").over(w_ordered).alias("next_txn_time_hour"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

kept_keys = (
    enriched
    .select("id", "merchant")
    .subtract(
        df_transactions
        .filter(F.col("amount") < 2000)
        .select("id", "merchant")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter((~F.col("merchant").eqNullSafe(F.lit("Grab"))) & (F.col("txn_time_hour") <= 47))
    .withColumn("is_flagged_flag_per_txn_time_hour", (F.col("is_flagged_flag") / F.nullif("txn_time_hour", F.lit(0))))
    .withColumn("next_txn_time_hour_whole", F.ceil("next_txn_time_hour"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("UNKNOWN")))
)

metrics = cleaned.select("*", F.percent_rank().over(w2).alias("is_flagged_flag_percent_rank"))

enriched_transactions = (
    metrics
    .select(
        "id",
        "is_flagged",
        "is_flagged_flag",
        "txn_time_hour",
        "prev_is_flagged_flag",
        "is_flagged_flag_percent_rank",
        F.count("is_flagged_flag").over(w3).alias("account_id_count_is_flagged_flag"),
        F.dense_rank().over(w4).alias("prev_is_flagged_flag_dense_rank"),
        F.dense_rank().over(w5).alias("row_seq_dense_rank"),
        F.min("txn_time_hour").over(w6).alias("merchant_min_txn_time_hour"),
    )
)

filtered = (
    enriched_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "is_flagged",
        "is_flagged_flag",
        "txn_time_hour",
        "prev_is_flagged_flag",
        "is_flagged_flag_percent_rank",
        "account_id_count_is_flagged_flag",
        "prev_is_flagged_flag_dense_rank",
        "merchant_min_txn_time_hour",
        F.when(F.col("account_id_count_is_flagged_flag") >= 13, "High").when(F.col("account_id_count_is_flagged_flag") >= 8, "Medium").otherwise("Low").alias("account_id_count_is_flagged_flag_tier"),
    )
)

result = (
    filtered
    .select(
        "id",
        "is_flagged",
        "prev_is_flagged_flag",
        "is_flagged_flag_percent_rank",
        "prev_is_flagged_flag_dense_rank",
    )
)
