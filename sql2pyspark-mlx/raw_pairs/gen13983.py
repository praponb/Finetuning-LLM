from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-06-20 18:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)

result = (
    cleaned
    .groupBy(
        "txn_type",
        F.when(F.col("is_flagged_flag") >= 68, "LARGE").otherwise("SMALL").alias("is_flagged_flag_tier"),
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
    )
    .agg(
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.round(F.avg("is_flagged_flag"), 2).alias("avg_is_flagged_flag"),
    )
    .filter(F.col("avg_is_flagged_flag") > 0)
)
