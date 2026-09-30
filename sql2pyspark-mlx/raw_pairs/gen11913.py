from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("order_id", "method").orderBy("paid_at", "id").rowsBetween(-6, -1)
w3 = Window.orderBy("paid_at", "id").rowsBetween(-2, -1)
w_recent = Window.orderBy("amount_sum_90d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_payments
    .filter(F.col("order_id").isin([6, 7, 25]))
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        F.sum("amount").over(w).alias("amount_sum_90d"),
        F.avg("amount").over(w2).alias("amount_avg_6_prior"),
        F.avg("amount").over(w3).alias("amount_avg_2_prior"),
    )
)

filtered = (
    enriched
    .filter((F.col("paid_at") >= F.lit("2023-05-21").cast("date")) & F.col("method").isNotNull())
    .withColumn("method_length", F.length("method"))
    .withColumn("amount_avg_2_prior_whole", F.floor("amount_avg_2_prior"))
    .withColumn("method_filled", F.coalesce("method", F.lit("UNKNOWN")))
)

valid = (
    filtered
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-06-15"))) & (~F.col("method").eqNullSafe(F.lit("promptpay"))))
    .select(
        "id",
        "method",
        "amount",
        "amount_sum_90d",
        "amount_avg_6_prior",
        "amount_avg_2_prior",
        "method_length",
        "amount_avg_2_prior_whole",
        "method_filled",
        F.date_trunc("WEEK", "paid_at").alias("paid_at_period"),
    )
)

cleaned = (
    valid
    .filter(F.col("method").eqNullSafe(F.lit("card")))
    .select(
        "id",
        "amount",
        "amount_sum_90d",
        "amount_avg_6_prior",
        "amount_avg_2_prior",
        "paid_at_period",
        F.col("amount_avg_2_prior_whole").cast("string").alias("amount_avg_2_prior_whole_str"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.max("amount_avg_2_prior").over(w_recent).alias("running_max_amount_avg_2_prior"),
        F.avg("amount_avg_2_prior").over(w_recent).alias("running_avg_amount_avg_2_prior"),
        F.count("amount_avg_2_prior").over(w_recent).alias("running_count_amount_avg_2_prior"),
    )
)

matched_keys = (
    with_windows
    .select("id")
    .subtract(
        df_payments
        .filter(F.col("method") == "card")
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

summary = (
    kept_rows
    .groupBy(F.date_trunc("MONTH", "paid_at_period").alias("activity_month"))
    .agg(
        F.percentile_approx("amount_avg_6_prior", 0.5).alias("amount_avg_6_prior_p50"),
        ((F.unix_timestamp(F.max("paid_at_period")) - F.unix_timestamp(F.min("paid_at_period"))) / 60).alias("span_minutes"),
        F.count("*").alias("event_count"),
        F.percentile_approx("amount_sum_90d", 0.9).alias("amount_sum_90d_p90"),
    )
)

result = (
    summary
    .select("activity_month", "span_minutes", "amount_sum_90d_p90")
    .orderBy(F.col("amount_sum_90d_p90").desc_nulls_first())
)
