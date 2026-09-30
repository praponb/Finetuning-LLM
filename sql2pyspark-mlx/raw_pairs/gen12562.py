from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy("amount", "id")
w2 = Window.partitionBy("method").orderBy("seq_no", "id")

valid = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]) & (F.col("paid_at") >= F.lit("2023-05-14 10:00:00").cast("timestamp")))
    .withColumn("method_status", F.concat("method", F.lit(", "), "status"))
    .withColumn("status_length", F.length("status"))
)

with_windows = (
    valid
    .withColumn("prev_status_length", F.lag("status_length").over(w))
    .withColumn("next_status_length", F.lead("status_length").over(w))
    .withColumn("next_amount", F.lead("amount").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)

cleaned = (
    with_windows
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-11-05"))) & F.col("seq_no").between(4, 6))
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        "method_status",
        "status_length",
        "next_status_length",
        "next_amount",
        "seq_no",
        (F.col("seq_no") + F.col("next_amount")).alias("seq_no_plus_next_amount"),
        F.concat_ws(",", "method", "status").alias("method_status_key"),
        F.coalesce("method", F.lit("NONE")).alias("method_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

cleaned_payments = (
    deduped
    .filter(F.col("paid_at") >= F.lit("2023-05-31 15:00:00").cast("timestamp"))
    .select(
        "id",
        "paid_at",
        "status",
        "status_length",
        "next_status_length",
        "next_amount",
        "seq_no_plus_next_amount",
        F.least("seq_no_plus_next_amount", "status_length").alias("least_seq_no_plus_next_amount_status_length"),
        F.greatest("next_status_length", "amount").alias("greatest_next_status_length_amount"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_clean"),
    )
)

stats = (
    cleaned_payments
    .groupBy(
        F.when(F.col("status_length") >= 74, "PREMIUM").otherwise("STANDARD").alias("status_length_tier"),
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
    )
    .agg(
        F.percentile_approx("next_amount", [0.25, 0.5, 0.75]).alias("next_amount_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.min("paid_at").alias("first_seen"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("next_status_length")).otherwise(0)).alias("failed_next_status_length"),
    )
)

result = (
    stats
    .select(
        "status_length_tier",
        "week_start",
        "next_amount_quartiles",
        "num_status",
        "first_seen",
        "failed_next_status_length",
    )
    .orderBy(F.col("failed_next_status_length").desc_nulls_last())
)
