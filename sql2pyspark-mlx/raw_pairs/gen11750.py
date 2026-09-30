from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan", "status").orderBy("end_date", "id")
w_part = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan", "customer_id").orderBy(F.col("end_date").desc())
w_hist = Window.partitionBy("plan").orderBy("row_seq", "id", "subscription_id").rowsBetween(-2, Window.currentRow)
w_running = Window.partitionBy("status").orderBy("row_seq_rounded", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("subscription_plan").orderBy("row_seq", "id", "subscription_id")

enriched = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 120)
    .withColumn("next_monthly_fee", F.lead("monthly_fee").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w_part))
)

deduped = (
    enriched
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "next_monthly_fee",
        "row_seq",
    )
)

filtered = (
    deduped
    .filter((F.col("start_date") >= F.lit("2023-04-10").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("row_seq_rounded", F.round("row_seq", 1))
)

matched = (
    filtered
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.start_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee"),
    )
)

metrics = (
    matched
    .select(
        "*",
        F.count("monthly_fee").over(w_hist).alias("last3_count_monthly_fee"),
        F.min("monthly_fee").over(w_hist).alias("last3_min_monthly_fee"),
        F.sum("monthly_fee").over(w_hist).alias("last3_sum_monthly_fee"),
    )
)

enriched_subscriptions = (
    metrics
    .select(
        "id",
        "customer_id",
        "plan",
        "row_seq",
        "subscription_id",
        "subscription_plan",
        "last3_sum_monthly_fee",
        F.sum("subscription_id").over(w_running).alias("running_sum_subscription_id"),
        F.max("subscription_id").over(w_running).alias("running_max_subscription_id"),
        F.count("subscription_id").over(w_running).alias("running_count_subscription_id"),
    )
)

latest = (
    enriched_subscriptions
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy("plan", "customer_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.when(F.col("subscription_plan") == "premium", F.col("running_max_subscription_id"))).alias("premium_avg_running_max_subscription_id"),
        F.count_if(F.col("subscription_plan") == "standard").alias("standard_events"),
        F.min("running_sum_subscription_id").alias("min_running_sum_subscription_id"),
        F.sum(F.when(F.col("subscription_plan") == "premium", F.col("running_sum_subscription_id")).otherwise(0)).alias("premium_running_sum_subscription_id"),
        F.percentile_approx("running_max_subscription_id", [0.25, 0.5, 0.75]).alias("running_max_subscription_id_quartiles"),
    )
)
