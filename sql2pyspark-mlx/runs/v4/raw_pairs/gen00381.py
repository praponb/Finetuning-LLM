from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id", "merchant").orderBy(F.col("amount").desc_nulls_last(), "id")
w4 = Window.orderBy(F.col("session_no").desc(), "id")
w5 = Window.partitionBy("txn_type").orderBy("amount", "id")
w6 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")
w7 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w8 = Window.partitionBy("merchant").orderBy("new_group_flag", "id")
w9 = Window.partitionBy("account_type").orderBy("txn_time", "id")
w10 = Window.partitionBy("account_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("year_start").orderBy(F.col("fee_new_group_flag_count_7d").desc())

filtered = (
    df_transactions
    .filter(F.col("account_id").isin([15, 17, 25, 29]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.hour("txn_time").alias("txn_time_hour"),
    )
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_group_flag").over(w2))

deduped = (
    sessionized
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    deduped
    .select(
        "*",
        F.row_number().over(w4).alias("event_seq"),
        F.ntile(10).over(w5).alias("amount_bucket"),
        F.row_number().over(w6).alias("row_seq"),
        F.count("new_group_flag").over(w7).alias("new_group_flag_count_7d"),
        F.lead("new_group_flag", 1).over(w8).alias("next_new_group_flag"),
    )
)

enriched = (
    scored
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.status"), F.col("d.account_type"))
)

gaps = (
    enriched
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w9).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w9) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized_transactions = gaps.withColumn("island_id", F.sum("session_start_flag").over(w10))

matched_keys = (
    sessionized_transactions
    .select("id", "merchant")
    .subtract(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id", "merchant")
    )
)

verified = (
    sessionized_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
        F.when(F.col("new_group_flag") >= 1, "LARGE").otherwise("SMALL").alias("new_group_flag_tier"),
        F.when(F.col("amount") >= 3200, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("new_group_flag_count_7d")).otherwise(0)).alias("fee_new_group_flag_count_7d"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "OPEN", 1)), F.lit(0)), 2).alias("amount_per_open"),
        F.percentile_approx("next_new_group_flag", [0.25, 0.5, 0.75]).alias("next_new_group_flag_quartiles"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
    )
    .withColumn("fee_new_group_flag_count_7d_dense_rank", F.dense_rank().over(w11))
    .orderBy("duration_min")
)
