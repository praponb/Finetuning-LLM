from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("plan").orderBy("running_max_monthly_fee", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("plan").orderBy(F.col("running_avg_monthly_fee").desc(), "id")
w4 = Window.partitionBy("plan", "status").orderBy("running_min_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy("running_avg_monthly_fee_avg_6_prior", "id")
w6 = Window.partitionBy("plan").orderBy("running_running_avg_monthly_fee")
w7 = Window.partitionBy("status").orderBy(F.col("seq_no_plus_running_min_monthly_fee").desc(), "id")

base = (
    df_subscriptions
    .filter(F.col("plan") != "basic")
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.trim("plan").alias("clean_plan"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "plan",
        "status",
        "clean_plan",
        F.count("monthly_fee").over(w_recent).alias("running_count_monthly_fee"),
        F.min("monthly_fee").over(w_recent).alias("running_min_monthly_fee"),
        F.max("monthly_fee").over(w_recent).alias("running_max_monthly_fee"),
        F.avg("monthly_fee").over(w_recent).alias("running_avg_monthly_fee"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "plan",
        "status",
        "clean_plan",
        "running_min_monthly_fee",
        "running_max_monthly_fee",
        "running_avg_monthly_fee",
        F.avg("running_avg_monthly_fee").over(w_hist).alias("running_avg_monthly_fee_avg_6_prior"),
        F.row_number().over(w3).alias("seq_no"),
        F.sum("running_avg_monthly_fee").over(w4).alias("running_running_avg_monthly_fee"),
    )
)

filtered = (
    metrics
    .filter(F.col("seq_no") != 8)
    .select(
        "id",
        "plan",
        "status",
        "clean_plan",
        "running_min_monthly_fee",
        "running_avg_monthly_fee_avg_6_prior",
        "seq_no",
        "running_running_avg_monthly_fee",
        (F.col("seq_no") + F.col("running_min_monthly_fee")).alias("seq_no_plus_running_min_monthly_fee"),
        F.round((F.col("running_avg_monthly_fee") * 100), 2).alias("running_avg_monthly_fee_scaled"),
    )
)

scored = (
    filtered
    .filter(F.col("seq_no_plus_running_min_monthly_fee") >= 37)
    .select(
        "*",
        F.lag("running_running_avg_monthly_fee").over(w5).alias("prev_running_running_avg_monthly_fee"),
        F.lag("seq_no").over(w5).alias("prev_seq_no"),
        F.row_number().over(w5).alias("seq_no_2"),
    )
)

ranked = (
    scored
    .withColumn("latest_rank", F.rank().over(w6))
    .filter(F.col("latest_rank") == 1)
)

ranked_subscriptions = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

stats = (
    ranked_subscriptions
    .groupBy("plan")
    .agg(
        F.max("prev_seq_no").alias("max_prev_seq_no"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
    )
)

result = (
    stats
    .select(
        "plan",
        "max_prev_seq_no",
        "seq_no_quartiles",
        F.when(F.col("plan").rlike("^(enterprise|standard)$"), "EXTENDED").when(F.col("plan").rlike("^(basic|premium)$"), "PRIORITY").otherwise("GROUP_A").alias("plan_zone"),
    )
    .orderBy(F.col("plan_zone").asc_nulls_last())
)
