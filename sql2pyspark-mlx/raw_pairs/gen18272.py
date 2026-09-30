from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "start_date"), "status")
w2 = Window.partitionBy(F.month("start_date"), "status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("end_date", "id")
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("customer_id").orderBy("next_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("customer_id").orderBy("running_start_date_period_count_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("running_start_date_period_count_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("customer_id").orderBy("cumulative_monthly_fee", "id")
w10 = Window.partitionBy("plan", "status")
w11 = Window.partitionBy("plan").orderBy("running_min_next_monthly_fee_int", "id")
w12 = Window.orderBy(F.col("monthly_fee_share").desc_nulls_last())
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("discount").alias("min_order_discount"))

metrics = (
    df_subscriptions
    .select(
        "*",
        F.count("monthly_fee").over(w).alias("start_date_period_count_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("cumulative_monthly_fee"),
        F.lead("monthly_fee", 1).over(w3).alias("next_monthly_fee"),
    )
)

filtered = (
    metrics
    .filter(F.col("start_date") >= F.lit("2023-06-14").cast("date"))
    .withColumn("next_monthly_fee_int", F.col("next_monthly_fee").cast("int"))
    .withColumn("next_monthly_fee_minus_start_date_period_count_monthly_fee", (F.col("next_monthly_fee") - F.col("start_date_period_count_monthly_fee")))
)

scored = (
    filtered
    .filter(F.col("start_date_period_count_monthly_fee").between(9, 12))
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w4)), 4).alias("monthly_fee_share"),
        F.sum("start_date_period_count_monthly_fee").over(w5).alias("running_start_date_period_count_monthly_fee"),
        F.sum("cumulative_monthly_fee").over(w_grp).alias("cumulative_cumulative_monthly_fee"),
    )
)

metrics_subscriptions = (
    scored
    .withColumn("running_avg_next_monthly_fee_int", F.avg("next_monthly_fee_int").over(w_hist))
    .withColumn("running_min_next_monthly_fee_int", F.min("next_monthly_fee_int").over(w_hist))
    .withColumn("running_max_next_monthly_fee_int", F.max("next_monthly_fee_int").over(w_hist))
)

scored_subscriptions = (
    metrics_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        "start_date_period_count_monthly_fee",
        "next_monthly_fee",
        "next_monthly_fee_int",
        "monthly_fee_share",
        "running_start_date_period_count_monthly_fee",
        "running_avg_next_monthly_fee_int",
        "running_min_next_monthly_fee_int",
        "running_max_next_monthly_fee_int",
        F.sum("start_date_period_count_monthly_fee").over(w8).alias("running_start_date_period_count_monthly_fee_2"),
        F.ntile(10).over(w9).alias("cumulative_monthly_fee_bucket"),
        F.stddev_samp("next_monthly_fee").over(w10).alias("plan_stddev_samp_next_monthly_fee"),
        F.ntile(5).over(w11).alias("running_min_next_monthly_fee_int_bucket"),
        F.dense_rank().over(w12).alias("monthly_fee_share_dense_rank"),
    )
)

result = (
    scored_subscriptions
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        "start_date_period_count_monthly_fee",
        "next_monthly_fee_int",
        "monthly_fee_share",
        "running_start_date_period_count_monthly_fee",
        F.col("min_order_discount"),
    )
)
