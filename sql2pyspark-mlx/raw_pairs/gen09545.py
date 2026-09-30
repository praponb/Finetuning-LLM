from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("start_date"), "plan")
w_running = Window.partitionBy("customer_id", "status").orderBy(F.col("monthly_fee").desc_nulls_last())
w3 = Window.partitionBy("status").orderBy("end_date", "id")
w4 = Window.partitionBy("customer_id", "status").orderBy(F.col("monthly_fee").desc(), "id")

scored = (
    df_subscriptions
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "status",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.rank().over(w_running).alias("monthly_fee_rank"),
        F.lag("monthly_fee", 1).over(w3).alias("prev_monthly_fee"),
        F.ntile(10).over(w4).alias("monthly_fee_bucket"),
    )
)

result = (
    scored
    .withColumn("monthly_fee_bucket_tier", F.when(F.col("monthly_fee_bucket") >= 7, "HIGH").otherwise("LOW"))
    .cube("monthly_fee_bucket_tier")
    .agg(
        F.percentile_approx("monthly_fee_rank", 0.9).alias("monthly_fee_rank_p90"),
        F.round(F.avg("prev_monthly_fee"), 2).alias("avg_prev_monthly_fee"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_rows"),
        F.round(F.avg("monthly_fee_rank"), 2).alias("avg_monthly_fee_rank"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("grouping_id"),
    )
)
