from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("monthly_fee_whole", "id").rowsBetween(-5, -1)
w_hist = Window.partitionBy("status").orderBy("monthly_fee", "id")
w4 = Window.partitionBy(F.date_trunc("WEEK", "start_date"), "status").orderBy("last4_avg_monthly_fee_whole", "id")
w5 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("last4_count_monthly_fee_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy(F.col("monthly_fee_avg_5_prior").desc_nulls_first(), "id")

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.max("monthly_fee_whole").over(w).alias("last4_max_monthly_fee_whole"),
        F.avg("monthly_fee_whole").over(w).alias("last4_avg_monthly_fee_whole"),
        F.count("monthly_fee_whole").over(w).alias("last4_count_monthly_fee_whole"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_5_prior"),
    )
)

with_windows_subscriptions = (
    with_windows
    .filter(F.col("status").isin(["ACTIVE", "EXPIRED", "CANCELLED"]))
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("last4_avg_monthly_fee_whole_bucket", F.ntile(10).over(w4))
    .withColumn("first_known_last4_max_monthly_fee_whole", F.first("last4_max_monthly_fee_whole", ignorenulls=True).over(w5))
    .withColumn("cumulative_last4_avg_monthly_fee_whole", F.sum("last4_avg_monthly_fee_whole").over(w6))
)

ranked = (
    with_windows_subscriptions
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .withColumn("monthly_fee_avg_5_prior_tier", F.when(F.col("monthly_fee_avg_5_prior") >= 250, "HIGH").otherwise("LOW"))
    .rollup("monthly_fee_avg_5_prior_tier", "seq_no")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("num_status") >= 1)
)
