from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("start_date"), "plan")
w2 = Window.partitionBy(F.date_trunc("WEEK", "start_date"), "status").orderBy("monthly_fee", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy(F.month("start_date"), "customer_id").orderBy("start_date", "id")
w4 = Window.partitionBy("plan").orderBy(F.col("start_date_month_avg_monthly_fee_int").desc_nulls_last(), "id")

valid = (
    df_subscriptions
    .filter(F.col("end_date") >= F.lit("2023-04-28").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
        F.substring("plan", 1, 2).alias("plan_prefix"),
    )
)

scored = (
    valid
    .filter(F.col("customer_id").isNull())
    .select(
        "*",
        F.avg("monthly_fee_int").over(w).alias("start_date_month_avg_monthly_fee_int"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_6"),
        F.lag("monthly_fee_int", 1).over(w3).alias("prev_monthly_fee_int"),
    )
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

valid_subscriptions = (
    ranked
    .filter((F.col("start_date") >= F.lit("2023-03-21").cast("date")) & F.col("customer_id").isin([8, 15]))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        "monthly_fee_int",
        "plan_prefix",
        "monthly_fee_avg_6",
        "prev_monthly_fee_int",
        "latest_rank",
        F.round("monthly_fee_int", 1).alias("monthly_fee_int_rounded"),
    )
)

result = (
    valid_subscriptions
    .groupBy(F.date_trunc("WEEK", "start_date").alias("week_start"))
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_rows"),
    )
    .filter(F.col("basic_events") >= 1)
    .orderBy(F.col("distinct_customer_ids").desc_nulls_last())
)
