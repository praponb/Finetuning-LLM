from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("weeks_end_date_to_start_date_tier").orderBy(F.col("expired_monthly_fee_int").desc())

cleaned = (
    df_subscriptions
    .filter(F.col("end_date") < "2024-08-01")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        (F.datediff("start_date", "end_date") / 7).alias("weeks_end_date_to_start_date"),
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("weeks_end_date_to_start_date") >= 84, "LARGE").otherwise("SMALL").alias("weeks_end_date_to_start_date_tier"),
        "plan",
    )
    .agg(
        F.percentile_approx("monthly_fee", 0.9).alias("monthly_fee_p90"),
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee_int")).otherwise(0)).alias("expired_monthly_fee_int"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("plan") == "premium").alias("premium_events"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
    )
    .withColumn("expired_monthly_fee_int_rank", F.rank().over(w))
)
