from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan", "status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("plan").orderBy("end_date", "id")
w4 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w5 = Window.orderBy(F.col("monthly_fee").desc_nulls_last())
w6 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())
w7 = Window.partitionBy(F.month("start_date"), "status").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_subscriptions
    .filter(F.col("plan") == "enterprise")
    .withColumn("running_max_monthly_fee", F.max("monthly_fee").over(w_hist))
    .withColumn("running_avg_monthly_fee", F.avg("monthly_fee").over(w_hist))
    .withColumn("running_count_monthly_fee", F.count("monthly_fee").over(w_hist))
    .withColumn("running_min_monthly_fee", F.min("monthly_fee").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w2))
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        "running_avg_monthly_fee",
    )
)

deduped = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "status",
        "running_avg_monthly_fee",
        "latest_rank",
        F.rank().over(w5).alias("monthly_fee_rank"),
        F.percent_rank().over(w6).alias("monthly_fee_percent_rank"),
        F.sum("running_avg_monthly_fee").over(w7).alias("running_running_avg_monthly_fee"),
    )
)

filtered = (
    with_windows
    .filter((F.col("start_date") > F.lit("2023-09-05").cast("date")) & (F.year("start_date") >= 2024))
    .select("*", F.lower("status").alias("status_lower"))
)

remaining_keys = (
    filtered
    .select("status", "id")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") <= "2025-08-01")
        .select("status", "id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .cube(F.date_trunc("YEAR", "start_date").alias("year_start"), "status")
    .agg(
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("status").asc_nulls_last())
)
