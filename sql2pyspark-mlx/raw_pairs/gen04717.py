from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("monthly_fee").asc_nulls_first())
w2 = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w4 = Window.partitionBy("plan").orderBy("end_date", "id")

with_windows = (
    df_subscriptions
    .select(
        "*",
        F.cume_dist().over(w).alias("monthly_fee_cume_dist"),
        F.sum("monthly_fee").over(w2).alias("cumulative_monthly_fee"),
        F.lag("monthly_fee", 1).over(w3).alias("prev_monthly_fee"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "plan", "start_date", "cumulative_monthly_fee", "event_seq")
)

kept_keys = (
    latest
    .select("id", "plan")
    .intersect(
        df_subscriptions
        .filter(F.col("customer_id").isNotNull())
        .select("id", "plan")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("start_date") >= "2024-05-19")
    .withColumn("plan_title", F.initcap(F.trim("plan")))
    .withColumn("is_premium", F.when(F.col("plan") == "premium", 1).otherwise(0))
)

result = (
    filtered
    .rollup(
        F.when(F.col("event_seq") >= 9, "HIGH").otherwise("LOW").alias("event_seq_tier"),
        "plan",
        F.date_trunc("QUARTER", "start_date").alias("quarter_start"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("is_premium", 0.5).alias("is_premium_p50"),
        F.sum("event_seq").alias("total_event_seq"),
        F.avg("is_premium").alias("avg_is_premium"),
    )
    .filter(F.col("is_premium_p50") >= 0)
    .orderBy(F.col("total_event_seq").desc())
)
