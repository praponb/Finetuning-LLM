from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").asc_nulls_first(), "id")
w2 = Window.partitionBy("merchant").orderBy("amount", "id")

ranked = (
    df_transactions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
    )
)

latest = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

kept_keys = (
    latest
    .select("account_id")
    .subtract(
        df_transactions
        .filter(F.col("txn_time") < "2024-07-01")
        .select("account_id")
    )
)

verified = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter((F.col("txn_time") >= F.lit("2023-09-29 22:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("N/A")))
)

valid = (
    cleaned
    .filter((F.col("txn_time") > F.lit("2023-06-10 01:00:00").cast("timestamp")) & F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "merchant_filled",
        F.round((F.col("amount") * 100), 2).alias("amount_adjusted"),
        F.upper("txn_type").alias("txn_type_upper"),
    )
)

result = (
    valid
    .groupBy("account_id")
    .pivot("merchant", ["7-Eleven", "Lazada", "Starbucks"])
    .agg(F.max("amount_adjusted").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "account_id",
        F.col("7-Eleven_value_sum").alias("v_7_eleven_value_sum"),
        F.col("7-Eleven_n").alias("v_7_eleven_n"),
        F.col("Lazada_value_sum").alias("lazada_value_sum"),
        F.col("Lazada_n").alias("lazada_n"),
        F.col("Starbucks_value_sum").alias("starbucks_value_sum"),
        F.col("Starbucks_n").alias("starbucks_n"),
    )
)
