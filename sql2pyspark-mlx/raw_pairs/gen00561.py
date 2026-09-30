from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("plan", "status").orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("monthly_fee_int", F.col("monthly_fee").cast("int"))
)

scored = (
    base
    .select(
        "*",
        F.avg("monthly_fee_int").over(w).alias("last3_avg_monthly_fee_int"),
        F.sum("monthly_fee_int").over(w).alias("last3_sum_monthly_fee_int"),
        F.max("monthly_fee_int").over(w).alias("last3_max_monthly_fee_int"),
        F.ntile(5).over(w2).alias("monthly_fee_bucket"),
    )
)

base_subscriptions = (
    scored
    .filter((F.col("start_date") > F.lit("2023-03-28").cast("date")) & (F.col("last3_max_monthly_fee_int") > 78))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "monthly_fee_int",
        "last3_avg_monthly_fee_int",
        "last3_sum_monthly_fee_int",
        "monthly_fee_bucket",
        F.concat_ws(",", "status", "plan").alias("status_plan_key"),
    )
)

with_windows = (
    base_subscriptions
    .withColumn("running_sum_last3_sum_monthly_fee_int", F.sum("last3_sum_monthly_fee_int").over(w3))
    .withColumn("running_avg_last3_sum_monthly_fee_int", F.avg("last3_sum_monthly_fee_int").over(w3))
    .withColumn("running_max_last3_sum_monthly_fee_int", F.max("last3_sum_monthly_fee_int").over(w3))
)

result = (
    with_windows
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .groupingSets(
        [["city", "plan"], ["city"]],
        F.col("c.city").alias("city"),
        F.col("src.plan").alias("plan"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("src.monthly_fee_int")), 2).alias("avg_monthly_fee_int"),
        F.avg(F.col("src.running_max_last3_sum_monthly_fee_int")).alias("avg_running_max_last3_sum_monthly_fee_int"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("avg_running_max_last3_sum_monthly_fee_int").desc())
)
