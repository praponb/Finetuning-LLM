from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy("start_date", "id")
w2 = Window.partitionBy(F.month("start_date"), "customer_id").orderBy(F.col("prev_monthly_fee").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("prev_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("customer_id").orderBy(F.col("prev_monthly_fee").desc())
w5 = Window.partitionBy("status").orderBy(F.col("prev_monthly_fee").desc(), "id")
w6 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("prev_monthly_fee_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status", "plan")
w9 = Window.partitionBy("plan", "customer_id").orderBy(F.col("prev_monthly_fee_dense_rank").asc_nulls_last(), "id")
w10 = Window.partitionBy("plan").orderBy(F.col("cumulative_prev_monthly_fee_bucket_2").desc(), "id")
w_running = Window.partitionBy("status").orderBy(F.col("cumulative_prev_monthly_fee_bucket_2").desc(), "id")
w12 = Window.partitionBy("plan", "status").orderBy("prev_monthly_fee", "id")

with_windows = (
    df_subscriptions
    .select(
        "*",
        F.lag("monthly_fee").over(w).alias("prev_monthly_fee"),
        F.row_number().over(w).alias("row_seq"),
    )
)

scored = (
    with_windows
    .withColumn("prev_monthly_fee_bucket", F.ntile(5).over(w2))
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w3))
    .withColumn("prev_monthly_fee_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("prev_monthly_fee_bucket_2", F.ntile(10).over(w5))
)

metrics = (
    scored
    .filter(F.col("row_seq") <= 4)
    .select(
        "*",
        F.sum("row_seq").over(w6).alias("running_row_seq"),
        F.sum("prev_monthly_fee_bucket_2").over(w7).alias("cumulative_prev_monthly_fee_bucket_2"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w8)), 4).alias("monthly_fee_share"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w9))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "prev_monthly_fee",
        "last_known_monthly_fee",
        "prev_monthly_fee_dense_rank",
        "running_row_seq",
        "cumulative_prev_monthly_fee_bucket_2",
        "monthly_fee_share",
    )
)

result = (
    ranked
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "prev_monthly_fee",
        "last_known_monthly_fee",
        "running_row_seq",
        F.row_number().over(w10).alias("seq_no"),
        F.ntile(5).over(w_running).alias("cumulative_prev_monthly_fee_bucket_2_bucket"),
        F.ntile(10).over(w_running).alias("cumulative_prev_monthly_fee_bucket_2_bucket_2"),
        F.lead("cumulative_prev_monthly_fee_bucket_2", 2).over(w12).alias("next_cumulative_prev_monthly_fee_bucket_2"),
    )
)
