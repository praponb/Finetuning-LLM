from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan", "customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())
w_recent = Window.orderBy("end_date", "id").rowsBetween(-3, -1)
w3 = Window.orderBy(F.col("monthly_fee").desc_nulls_first())
w4 = Window.orderBy(F.col("monthly_fee").desc(), "id")
w5 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("first_known_monthly_fee_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.date_trunc("YEAR", "end_date")).orderBy("running_first_known_monthly_fee_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("monthly_fee_cume_dist").desc(), "id")
w10 = Window.orderBy("first_known_monthly_fee_whole", "id")
w_grp = Window.orderBy("end_date", "id")
w12 = Window.orderBy("running_first_known_monthly_fee_whole", "id")
w13 = Window.orderBy("monthly_fee_bucket", "id")
w14 = Window.partitionBy(F.month("end_date")).orderBy("monthly_fee_bucket", "id")
w15 = Window.orderBy("monthly_fee_percent_rank", "id")
w16 = Window.orderBy("running_first_known_monthly_fee_whole_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w17 = Window.orderBy("least_row_seq_monthly_fee_cume_dist_bucket", "id").rowsBetween(-3, -1)
w18 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w19 = Window.orderBy(F.col("monthly_fee").desc())

cleaned = (
    df_subscriptions
    .filter(F.col("start_date") > F.lit("2023-06-29").cast("date"))
    .select("*", F.floor("monthly_fee").alias("monthly_fee_whole"))
)

metrics = (
    cleaned
    .select(
        "id",
        "end_date",
        "monthly_fee",
        F.percent_rank().over(w_part).alias("monthly_fee_percent_rank"),
        F.avg("monthly_fee").over(w_recent).alias("monthly_fee_avg_3_prior"),
        F.cume_dist().over(w3).alias("monthly_fee_cume_dist"),
        F.ntile(10).over(w4).alias("monthly_fee_bucket"),
        F.first("monthly_fee_whole", ignorenulls=True).over(w5).alias("first_known_monthly_fee_whole"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.sum("first_known_monthly_fee_whole").over(w6).alias("running_first_known_monthly_fee_whole"),
        F.sum("monthly_fee_cume_dist").over(w7).alias("running_monthly_fee_cume_dist"),
    )
)

metrics_subscriptions = (
    enriched
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "monthly_fee_percent_rank",
        "monthly_fee_bucket",
        "running_first_known_monthly_fee_whole",
        F.sum("monthly_fee_avg_3_prior").over(w8).alias("cumulative_monthly_fee_avg_3_prior"),
        F.ntile(3).over(w9).alias("monthly_fee_cume_dist_bucket"),
        F.lead("monthly_fee_bucket", 1).over(w10).alias("next_monthly_fee_bucket"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

scored = (
    metrics_subscriptions
    .select(
        "*",
        F.ntile(10).over(w12).alias("running_first_known_monthly_fee_whole_bucket"),
        F.lag("cumulative_monthly_fee_avg_3_prior", 1).over(w13).alias("prev_cumulative_monthly_fee_avg_3_prior"),
        F.lag("monthly_fee_cume_dist_bucket", 2, 0).over(w14).alias("prev_monthly_fee_cume_dist_bucket"),
        F.row_number().over(w15).alias("row_seq_2"),
    )
)

filtered = (
    scored
    .filter(F.col("prev_cumulative_monthly_fee_avg_3_prior") <= 371)
    .select(
        "*",
        F.least("row_seq", "monthly_fee_cume_dist_bucket").alias("least_row_seq_monthly_fee_cume_dist_bucket"),
        F.when(F.col("monthly_fee") >= 183, "High").when(F.col("monthly_fee") >= 88, "Medium").otherwise("Low").alias("monthly_fee_level"),
    )
)

metrics_subscriptions_2 = (
    filtered
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "monthly_fee_percent_rank",
        "running_first_known_monthly_fee_whole",
        "cumulative_monthly_fee_avg_3_prior",
        "monthly_fee_cume_dist_bucket",
        "next_monthly_fee_bucket",
        F.last("cumulative_monthly_fee_avg_3_prior", ignorenulls=True).over(w16).alias("last_known_cumulative_monthly_fee_avg_3_prior"),
        F.avg("cumulative_monthly_fee_avg_3_prior").over(w17).alias("cumulative_monthly_fee_avg_3_prior_avg_3_prior"),
        F.sum("running_first_known_monthly_fee_whole_bucket").over(w18).alias("cumulative_running_first_known_monthly_fee_whole_bucket"),
        F.dense_rank().over(w19).alias("monthly_fee_dense_rank"),
    )
)

result = (
    metrics_subscriptions_2
    .select(
        "id",
        "end_date",
        "monthly_fee_percent_rank",
        "running_first_known_monthly_fee_whole",
        "cumulative_monthly_fee_avg_3_prior",
        "monthly_fee_cume_dist_bucket",
        "next_monthly_fee_bucket",
        "cumulative_running_first_known_monthly_fee_whole_bucket",
        "monthly_fee_dense_rank",
    )
    .orderBy(F.col("id").desc())
)
