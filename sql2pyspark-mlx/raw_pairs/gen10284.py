from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter((~F.col("merchant").eqNullSafe(F.lit("Lazada"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged",
        F.substring("txn_type", 1, 4).alias("txn_type_prefix"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_filled"),
    )
)

result = (
    valid
    .withColumn("amount_tier", F.when(F.col("amount") >= 4400, "LARGE").otherwise("SMALL"))
    .rollup("account_id", "amount_tier")
    .agg(
        F.max("txn_time").alias("last_seen"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("span_minutes").desc_nulls_first())
)
