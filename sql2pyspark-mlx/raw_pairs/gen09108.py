from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("end_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("plan", "customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy(F.year("end_date"), "status")
w4 = Window.partitionBy("status")
w_running = Window.partitionBy("plan").orderBy("monthly_fee_bucket", "id")
w6 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(-6, Window.currentRow)

latest = (
    df_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    latest
    .filter(F.col("monthly_fee") >= 180)
    .select(
        "id",
        "plan",
        "end_date",
        F.ntile(10).over(w2).alias("monthly_fee_bucket"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w3)), 4).alias("monthly_fee_share"),
        F.count("monthly_fee").over(w4).alias("status_count_monthly_fee"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "end_date",
        "monthly_fee_share",
        F.lead("monthly_fee_share").over(w_running).alias("next_monthly_fee_share"),
        F.lead("monthly_fee_bucket").over(w_running).alias("next_monthly_fee_bucket"),
        F.row_number().over(w_running).alias("row_seq"),
        F.avg("monthly_fee_bucket").over(w6).alias("monthly_fee_bucket_avg_6"),
    )
)

base = (
    scored
    .filter(F.col("next_monthly_fee_bucket") > 8)
    .select(
        "id",
        "end_date",
        "next_monthly_fee_share",
        "row_seq",
        (F.col("next_monthly_fee_share") / F.nullif("monthly_fee_share", F.lit(0))).alias("next_monthly_fee_share_per_monthly_fee_share"),
    )
)

result = (
    base
    .select(
        "id",
        "end_date",
        "next_monthly_fee_share",
        "row_seq",
        "next_monthly_fee_share_per_monthly_fee_share",
    )
    .orderBy("end_date", "next_monthly_fee_share_per_monthly_fee_share")
)
