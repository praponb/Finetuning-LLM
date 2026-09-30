from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan", "status").orderBy(F.col("running_count_monthly_fee").asc_nulls_last(), "id")
w3 = Window.partitionBy("plan").orderBy("running_sum_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status", "customer_id").orderBy("running_min_running_min_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        F.count("monthly_fee").over(w).alias("running_count_monthly_fee"),
        F.min("monthly_fee").over(w).alias("running_min_monthly_fee"),
        F.max("monthly_fee").over(w).alias("running_max_monthly_fee"),
        F.sum("monthly_fee").over(w).alias("running_sum_monthly_fee"),
    )
)

valid = (
    with_windows
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") == 32))
    .select(
        "*",
        F.round("running_count_monthly_fee", 1).alias("running_count_monthly_fee_rounded"),
        F.coalesce("running_sum_monthly_fee", F.lit(0)).alias("running_sum_monthly_fee_filled"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

with_windows_subscriptions = (
    deduped
    .filter(F.col("running_count_monthly_fee_rounded").between(60, 75))
    .select(
        "*",
        F.sum("running_min_monthly_fee").over(w3).alias("running_sum_running_min_monthly_fee"),
        F.min("running_min_monthly_fee").over(w3).alias("running_min_running_min_monthly_fee"),
        F.count("running_min_monthly_fee").over(w3).alias("running_count_running_min_monthly_fee"),
    )
)

scored = (
    with_windows_subscriptions
    .select(
        "*",
        F.max("running_min_monthly_fee").over(w4).alias("running_max_running_min_monthly_fee"),
        F.avg("running_min_monthly_fee").over(w4).alias("running_avg_running_min_monthly_fee"),
        F.min("running_min_monthly_fee").over(w4).alias("running_min_running_min_monthly_fee_2"),
        F.count("running_min_monthly_fee").over(w4).alias("running_count_running_min_monthly_fee_2"),
    )
)

cleaned = (
    scored
    .filter(F.col("customer_id").isNotNull())
    .withColumn("running_min_monthly_fee_whole", F.ceil("running_min_monthly_fee"))
    .withColumn("dedup_rank_times_running_count_running_min_monthly_fee_2", (F.col("dedup_rank") * F.col("running_count_running_min_monthly_fee_2")))
)

result = (
    cleaned
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .groupingSets(
        [["city", "status"], ["city"]],
        F.col("p.city").alias("city"),
        F.col("s.status").alias("status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("s.running_count_monthly_fee_rounded")).alias("max_running_count_monthly_fee_rounded"),
        F.grouping_id().alias("gid"),
    )
)
