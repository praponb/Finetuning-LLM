from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date_month_end", "id")
w2 = Window.partitionBy("customer_id", "status").orderBy(F.col("months_since_start_date").asc_nulls_first())
w3 = Window.partitionBy("status", "plan").orderBy(F.col("start_date").asc_nulls_first(), "id")

base = (
    df_subscriptions
    .filter(F.col("start_date") > F.to_date(F.lit("2023-06-20")))
    .withColumn("end_date_month_end", F.last_day("end_date"))
    .withColumn("months_since_start_date", (F.datediff(F.current_date(), "start_date") / 30))
)

metrics = (
    base
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("months_since_start_date_cume_dist", F.cume_dist().over(w2))
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_window = (
    deduped
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.start_date") >= F.col("sub.start_date")) & (F.col("a.start_date") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

totals = (
    with_window
    .withColumn("months_since_start_date_cume_dist_tier", F.when(F.col("months_since_start_date_cume_dist") >= 0.83, "PREMIUM").otherwise("STANDARD"))
    .withColumn("seq_no_tier", F.when(F.col("seq_no") >= 8, "HIGH").otherwise("LOW"))
    .rollup("months_since_start_date_cume_dist_tier", "seq_no_tier", "customer_id")
    .agg(
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("subscription_plan")).alias("subscription_plan_list"),
        F.grouping("months_since_start_date_cume_dist_tier").alias("grouping_level"),
    )
)

result = (
    totals
    .select(
        "months_since_start_date_cume_dist_tier",
        "seq_no_tier",
        "customer_id",
        "row_count",
        "subscription_plan_list",
        "grouping_level",
    )
    .orderBy(F.col("seq_no_tier").desc())
)
