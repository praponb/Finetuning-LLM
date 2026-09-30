from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status", "customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("customer_id").orderBy("start_date", "id")
w3 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)
w_hist = Window.partitionBy("status", "plan").orderBy("last3_max_running_sum_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_subscriptions
    .filter(F.col("start_date") >= F.lit("2023-09-10").cast("date"))
    .select(
        "*",
        (F.datediff(F.current_date(), "start_date") / 30).alias("months_since_start_date"),
    )
)

metrics = (
    filtered
    .withColumn("running_sum_monthly_fee", F.sum("monthly_fee").over(w_grp))
    .withColumn("running_avg_monthly_fee", F.avg("monthly_fee").over(w_grp))
    .withColumn("running_count_monthly_fee", F.count("monthly_fee").over(w_grp))
    .withColumn("running_max_monthly_fee", F.max("monthly_fee").over(w_grp))
    .withColumn("event_seq", F.row_number().over(w_recent))
)

scored = (
    metrics
    .select(
        "*",
        F.count("running_sum_monthly_fee").over(w3).alias("last3_count_running_sum_monthly_fee"),
        F.max("running_sum_monthly_fee").over(w3).alias("last3_max_running_sum_monthly_fee"),
        F.sum("running_sum_monthly_fee").over(w3).alias("last3_sum_running_sum_monthly_fee"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        "months_since_start_date",
        "running_count_monthly_fee",
        "running_max_monthly_fee",
        "event_seq",
        F.avg("running_avg_monthly_fee").over(w_hist).alias("running_avg_running_avg_monthly_fee"),
        F.count("running_avg_monthly_fee").over(w_hist).alias("running_count_running_avg_monthly_fee"),
        F.min("running_avg_monthly_fee").over(w_hist).alias("running_min_running_avg_monthly_fee"),
    )
)

stats = (
    with_windows
    .withColumn("activity_month", F.date_trunc("month", "start_date"))
    .withColumn("week_start", F.date_trunc("week", "start_date"))
    .groupingSets(
        [["activity_month", "week_start"], ["activity_month"]],
        "activity_month",
        "week_start",
    )
    .agg(
        F.round(F.sum("months_since_start_date") / F.nullif(F.count(F.when(F.col("plan") == "standard", 1)), F.lit(0)), 2).alias("months_since_start_date_per_standard"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.max("running_count_running_avg_monthly_fee").alias("max_running_count_running_avg_monthly_fee"),
    )
)

result = (
    stats
    .select(
        "activity_month",
        "months_since_start_date_per_standard",
        "plan_list",
        "max_running_count_running_avg_monthly_fee",
    )
)
