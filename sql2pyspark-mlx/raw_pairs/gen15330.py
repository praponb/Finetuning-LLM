from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "start_date"), "status").orderBy("monthly_fee_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("monthly_fee", "id").rowsBetween(-2, -1)
w5 = Window.orderBy("prev_monthly_fee", "id")
w6 = Window.orderBy(F.col("seq_no").desc(), "id")
w7 = Window.orderBy("prev_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("first_known_prev_monthly_fee").desc(), "id")

filtered = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 110)
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
    )
)

enriched = (
    filtered
    .filter(F.col("start_date") > "2024-01-15")
    .select(
        "id",
        "monthly_fee",
        "monthly_fee_filled",
        F.lag("monthly_fee").over(w).alias("prev_monthly_fee"),
        F.lead("monthly_fee").over(w).alias("next_monthly_fee"),
        F.lag("monthly_fee_filled").over(w).alias("prev_monthly_fee_filled"),
        F.row_number().over(w).alias("seq_no"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.first("prev_monthly_fee", ignorenulls=True).over(w3).alias("first_known_prev_monthly_fee"),
        F.avg("running_monthly_fee").over(w4).alias("running_monthly_fee_avg_2_prior"),
        F.row_number().over(w5).alias("seq_no_2"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.row_number().over(w6).alias("row_seq"),
        F.sum("first_known_prev_monthly_fee").over(w7).alias("cumulative_first_known_prev_monthly_fee"),
        F.ntile(10).over(w8).alias("first_known_prev_monthly_fee_bucket"),
    )
)

matched_keys = (
    with_windows
    .select("id")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id").isNotNull())
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        "seq_no",
        F.when(F.col("prev_monthly_fee_filled") >= 88, "HIGH").otherwise("LOW").alias("prev_monthly_fee_filled_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("prev_monthly_fee", [0.25, 0.5, 0.75]).alias("prev_monthly_fee_quartiles"),
    )
)
