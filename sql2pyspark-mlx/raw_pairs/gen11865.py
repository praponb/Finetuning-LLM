from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee").desc(), "id")

filtered = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-04-25").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.round("monthly_fee", 1).alias("monthly_fee_rounded"))
)

latest = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    latest
    .filter(F.col("end_date") > F.to_date(F.lit("2023-10-08")))
    .withColumn("end_date_period", F.date_trunc("YEAR", "end_date"))
)

filtered_subscriptions = (
    cleaned
    .filter(F.col("customer_id").isNull() & (F.col("end_date") >= F.to_date(F.lit("2023-05-23"))))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee_rounded",
        "end_date_period",
        F.date_trunc("year", "start_date").alias("start_date_period"),
        (F.col("dedup_rank") - F.col("monthly_fee_rounded")).alias("dedup_rank_minus_monthly_fee_rounded"),
    )
)

result = (
    filtered_subscriptions
    .groupBy(F.date_trunc("YEAR", "end_date_period").alias("year_start"))
    .agg(
        F.round(F.avg("dedup_rank_minus_monthly_fee_rounded"), 2).alias("avg_dedup_rank_minus_monthly_fee_rounded"),
        F.round(F.sum("monthly_fee_rounded") / F.nullif(F.count(F.when(F.col("plan") == "premium", 1)), F.lit(0)), 2).alias("monthly_fee_rounded_per_premium"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.size(F.collect_set("plan")).alias("num_plans_2"),
    )
)
