from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan", "status").orderBy("end_date", "id")
w_running = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("end_date"), "plan")
w4 = Window.orderBy(F.col("prev_monthly_fee").desc())
w5 = Window.partitionBy(F.year("end_date"), "plan")

valid = (
    df_subscriptions
    .filter(~F.col("plan").isin(["basic", "standard"]))
    .withColumn("start_date_minus_33d", F.date_sub("start_date", 33))
)

metrics = (
    valid
    .filter(F.col("monthly_fee").isNotNull())
    .select(
        "id",
        "plan",
        "end_date",
        "start_date_minus_33d",
        F.lag("monthly_fee").over(w_hist).alias("prev_monthly_fee"),
        F.lead("monthly_fee").over(w_hist).alias("next_monthly_fee"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.last("monthly_fee", ignorenulls=True).over(w_running).alias("last_known_monthly_fee"),
    )
)

kept_keys = (
    metrics
    .select("id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("plan") == "enterprise")
        .select("id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics_subscriptions = (
    verified
    .select(
        "next_monthly_fee",
        "event_seq",
        "last_known_monthly_fee",
        F.sum("prev_monthly_fee").over(w3).alias("end_date_month_sum_prev_monthly_fee"),
        F.dense_rank().over(w4).alias("prev_monthly_fee_dense_rank"),
        F.sum("last_known_monthly_fee").over(w3).alias("end_date_month_sum_last_known_monthly_fee"),
        F.round((F.col("prev_monthly_fee") / F.sum("prev_monthly_fee").over(w5)), 4).alias("prev_monthly_fee_share"),
    )
)

result = (
    metrics_subscriptions
    .groupBy(
        F.when(F.col("prev_monthly_fee_dense_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("prev_monthly_fee_dense_rank_tier"),
        F.when(F.col("end_date_month_sum_last_known_monthly_fee") >= 1192, "HIGH").otherwise("LOW").alias("end_date_month_sum_last_known_monthly_fee_tier"),
    )
    .agg(
        F.percentile_approx("last_known_monthly_fee", [0.25, 0.5, 0.75]).alias("last_known_monthly_fee_quartiles"),
        F.min("event_seq").alias("min_event_seq"),
        F.min("end_date_month_sum_prev_monthly_fee").alias("min_end_date_month_sum_prev_monthly_fee"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("min_event_seq").desc_nulls_last())
)
