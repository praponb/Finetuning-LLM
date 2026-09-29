from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(-3, -1)
w3 = Window.orderBy("monthly_fee", "id")
w4 = Window.orderBy(F.col("monthly_fee_bucket_2").asc_nulls_first())
w5 = Window.orderBy("monthly_fee_avg_3_prior", "id")
w6 = Window.orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_bucket_2_rank").desc(), "id")
w8 = Window.partitionBy("plan").orderBy("monthly_fee_avg_3_prior", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.orderBy(F.col("monthly_fee_bucket_2").desc(), "id")
w10 = Window.partitionBy("plan").orderBy(F.col("prev_monthly_fee_bucket_2").desc())
w11 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_avg_3_prior").desc(), "id")

with_windows = (
    df_subscriptions
    .select(
        "*",
        F.ntile(5).over(w).alias("monthly_fee_bucket"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_3_prior"),
        F.ntile(4).over(w3).alias("monthly_fee_bucket_2"),
    )
)

with_windows_subscriptions = (
    with_windows
    .select(
        "*",
        F.rank().over(w4).alias("monthly_fee_bucket_2_rank"),
        F.lag("monthly_fee_bucket_2", 1).over(w5).alias("prev_monthly_fee_bucket_2"),
    )
)

valid = (
    with_windows_subscriptions
    .filter((F.col("end_date") > F.to_date(F.lit("2024-04-10"))) & F.col("customer_id").isNotNull())
    .withColumn("prev_monthly_fee_bucket_2_whole", F.ceil("prev_monthly_fee_bucket_2"))
)

with_windows_subscriptions_2 = (
    valid
    .select(
        "*",
        F.sum("prev_monthly_fee_bucket_2").over(w6).alias("running_prev_monthly_fee_bucket_2"),
        F.ntile(3).over(w7).alias("monthly_fee_bucket_2_rank_bucket"),
        F.avg("monthly_fee_avg_3_prior").over(w8).alias("monthly_fee_avg_3_prior_avg_6"),
        F.row_number().over(w9).alias("event_seq"),
    )
)

joined = (
    with_windows_subscriptions_2
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.segment"))
)

scored = (
    joined
    .select(
        "*",
        F.dense_rank().over(w10).alias("prev_monthly_fee_bucket_2_dense_rank"),
        F.first("prev_monthly_fee_bucket_2", ignorenulls=True).over(w11).alias("first_known_prev_monthly_fee_bucket_2"),
        F.row_number().over(w12).alias("seq_no"),
    )
)

result = (
    scored
    .groupBy("plan")
    .agg(
        F.sum("monthly_fee_avg_3_prior_avg_6").alias("total_monthly_fee_avg_3_prior_avg_6"),
        F.count_if(F.col("status") == "EXPIRED").alias("expired_events"),
        F.round(F.sum("prev_monthly_fee_bucket_2_dense_rank") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("prev_monthly_fee_bucket_2_dense_rank_per_basic"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_rows"),
    )
    .orderBy("prev_monthly_fee_bucket_2_dense_rank_per_basic")
)
