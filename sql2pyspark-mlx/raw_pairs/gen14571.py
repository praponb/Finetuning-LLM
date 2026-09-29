from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("monthly_fee").asc_nulls_last())
w3 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("plan", "status").orderBy("rank_in_group", "id")
w5 = Window.partitionBy("status").orderBy(F.col("monthly_fee_int").desc(), "id")

valid = (
    df_subscriptions
    .filter((F.col("status") == "CANCELLED") & F.col("customer_id").isNotNull())
    .withColumn("status_plan_key", F.concat_ws("-", "status", "plan"))
)

cleaned = (
    valid
    .filter(F.col("end_date") < "2025-08-01")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        "status_plan_key",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

with_windows = (
    latest
    .select(
        "*",
        F.sum("monthly_fee_int").over(w3).alias("cumulative_monthly_fee_int"),
        F.lag("monthly_fee_int", 1).over(w4).alias("prev_monthly_fee_int"),
        F.ntile(3).over(w5).alias("monthly_fee_int_bucket"),
    )
)

totals = (
    with_windows
    .withColumn("week_start", F.date_trunc("WEEK", "start_date"))
    .withColumn("monthly_fee_int_bucket_tier", F.when(F.col("monthly_fee_int_bucket") >= 8, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["week_start", "monthly_fee_int_bucket_tier"], ["week_start"]],
        "week_start",
        "monthly_fee_int_bucket_tier",
    )
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_rows"),
        F.count("*").alias("cnt"),
        F.avg("prev_monthly_fee_int").alias("avg_prev_monthly_fee_int"),
        F.sum(F.when(F.col("plan") == "standard", F.col("prev_monthly_fee_int"))).alias("standard_total_prev_monthly_fee_int"),
    )
    .filter(F.col("avg_prev_monthly_fee_int") > 0)
)

result = (
    totals
    .select(
        "week_start",
        "monthly_fee_int_bucket_tier",
        "cancelled_rows",
        "cnt",
        "avg_prev_monthly_fee_int",
        "standard_total_prev_monthly_fee_int",
    )
)
