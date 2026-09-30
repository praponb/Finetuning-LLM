from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.year("start_date"), "customer_id")
w3 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("plan").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status")
w_running = Window.orderBy("monthly_fee_avg_6_prior", "id")
w7 = Window.partitionBy("customer_id").orderBy("start_date_year_stddev_samp_monthly_fee", "id")
w8 = Window.partitionBy("customer_id").orderBy("last_known_monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("customer_id").orderBy("rank_in_group", "id")

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("start_date") >= F.lit("2023-09-10").cast("date")))
    .select("*", F.concat("plan", F.lit(" "), "status").alias("plan_status"))
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    deduped
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "rank_in_group",
        F.stddev_samp("monthly_fee").over(w2).alias("start_date_year_stddev_samp_monthly_fee"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_6_prior"),
        F.last("monthly_fee", ignorenulls=True).over(w4).alias("last_known_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w5)), 4).alias("monthly_fee_share"),
    )
)

metrics_subscriptions = (
    metrics
    .select(
        "id",
        "customer_id",
        "end_date",
        "rank_in_group",
        F.lag("monthly_fee_share", 2).over(w_running).alias("prev_monthly_fee_share"),
        F.lead("start_date_year_stddev_samp_monthly_fee", 2).over(w7).alias("next_start_date_year_stddev_samp_monthly_fee"),
        F.avg("start_date_year_stddev_samp_monthly_fee").over(w8).alias("start_date_year_stddev_samp_monthly_fee_avg_3"),
    )
)

latest = (
    metrics_subscriptions
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

remaining_keys = (
    latest
    .select("customer_id", "id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("status") == "PAUSED")
        .select("customer_id", "id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .cube("customer_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.grouping("customer_id").alias("grouping_level"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
