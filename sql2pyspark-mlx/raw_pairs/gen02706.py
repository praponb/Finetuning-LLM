from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id")
w2 = Window.partitionBy(F.month("start_date"), "status").orderBy(F.col("start_date").desc(), "id")
w3 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w4 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("row_seq", "id")
w6 = Window.partitionBy("status")
w7 = Window.partitionBy("status").orderBy("end_date")

scored = (
    df_subscriptions
    .filter(F.col("status") == "PAUSED")
    .select(
        "*",
        F.ntile(3).over(w).alias("monthly_fee_bucket"),
        F.row_number().over(w2).alias("row_seq"),
        F.ntile(3).over(w3).alias("monthly_fee_bucket_2"),
        F.avg("monthly_fee").over(w4).alias("monthly_fee_avg_3"),
    )
)

base = (
    scored
    .filter((F.year("start_date") >= 2023) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "monthly_fee_bucket",
        "row_seq",
        "monthly_fee_avg_3",
        (F.col("monthly_fee_avg_3") - F.col("row_seq")).alias("monthly_fee_avg_3_minus_row_seq"),
        F.round((F.col("monthly_fee_bucket_2") / F.col("row_seq")), 2).alias("monthly_fee_bucket_2_per_row_seq"),
    )
)

base_subscriptions = (
    base
    .filter(F.col("customer_id").isNotNull() & F.col("monthly_fee_avg_3_minus_row_seq").isNotNull())
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "monthly_fee_bucket",
        "row_seq",
        "monthly_fee_avg_3",
        "monthly_fee_avg_3_minus_row_seq",
        "monthly_fee_bucket_2_per_row_seq",
        F.floor("monthly_fee_avg_3").alias("monthly_fee_avg_3_whole"),
        F.round((F.datediff("end_date", "start_date") / 30), 1).alias("months_start_date_to_end_date"),
    )
)

filtered = (
    base_subscriptions
    .filter(F.col("monthly_fee_avg_3_whole") >= 29)
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "monthly_fee_bucket",
        "row_seq",
        "monthly_fee_avg_3",
        "monthly_fee_avg_3_minus_row_seq",
        "monthly_fee_bucket_2_per_row_seq",
        "months_start_date_to_end_date",
        (F.col("monthly_fee_avg_3_whole") * F.col("monthly_fee_avg_3_minus_row_seq")).alias("monthly_fee_avg_3_whole_times_monthly_fee_avg_3_minus_row_seq"),
        (F.col("months_start_date_to_end_date") * F.col("monthly_fee_bucket_2_per_row_seq")).alias("months_start_date_to_end_date_times_monthly_fee_bucket_2_per_row_seq"),
    )
)

with_windows = (
    filtered
    .withColumn("row_seq_bucket", F.ntile(5).over(w5))
    .withColumn("row_seq_share", F.round((F.col("row_seq") / F.sum("row_seq").over(w6)), 4))
)

latest = (
    with_windows
    .withColumn("row_num", F.rank().over(w7))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

cleaned = (
    latest
    .filter(F.col("end_date") > F.to_date(F.lit("2023-04-17")))
    .select(
        "id",
        "end_date",
        "status",
        "monthly_fee_bucket",
        "row_seq",
        "monthly_fee_avg_3",
        "monthly_fee_avg_3_minus_row_seq",
        "monthly_fee_bucket_2_per_row_seq",
        "months_start_date_to_end_date",
        "monthly_fee_avg_3_whole_times_monthly_fee_avg_3_minus_row_seq",
        F.round((F.col("monthly_fee_bucket_2_per_row_seq") * 0.85), 2).alias("monthly_fee_bucket_2_per_row_seq_adjusted"),
    )
)

result = (
    cleaned
    .cube(
        "status",
        F.date_trunc("QUARTER", "end_date").alias("quarter_start"),
        F.date_trunc("WEEK", "end_date").alias("week_start"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee_bucket_2_per_row_seq_adjusted")).otherwise(0)).alias("active_monthly_fee_bucket_2_per_row_seq_adjusted"),
        F.sum("months_start_date_to_end_date").alias("total_months_start_date_to_end_date"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_rows"),
    )
    .orderBy(F.col("active_rows").asc_nulls_first())
)
