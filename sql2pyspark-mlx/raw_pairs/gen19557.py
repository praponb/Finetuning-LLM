from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w_ordered = Window.partitionBy(F.date_trunc("WEEK", "end_date"), "status")
w3 = Window.partitionBy("plan").orderBy(F.col("end_date_period_stddev_samp_end_date_day").desc_nulls_first())
w4 = Window.partitionBy("customer_id", "status").orderBy(F.col("start_date").desc(), "id")
w5 = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy("end_date_period_stddev_samp_end_date_day", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("status", "customer_id").orderBy("last4_sum_end_date_day", "id")
w7 = Window.partitionBy("status").orderBy("signup_date", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-03-18").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("end_date_day", F.dayofmonth("end_date"))
)

metrics = (
    valid
    .filter(F.col("end_date").between("2024-02-01", "2025-05-09"))
    .withColumn("last4_sum_end_date_day", F.sum("end_date_day").over(w_part))
    .withColumn("last4_count_end_date_day", F.count("end_date_day").over(w_part))
    .withColumn("last4_min_end_date_day", F.min("end_date_day").over(w_part))
    .withColumn("end_date_period_stddev_samp_end_date_day", F.stddev_samp("end_date_day").over(w_ordered))
)

with_dims = (
    metrics
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.signup_date"), F.col("d.city"))
)

with_windows = (
    with_dims
    .withColumn("end_date_period_stddev_samp_end_date_day_rank", F.rank().over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("monthly_fee_avg_6", F.avg("monthly_fee").over(w5))
    .withColumn("next_last4_sum_end_date_day", F.lead("last4_sum_end_date_day", 1, 0).over(w6))
    .withColumn("monthly_fee_avg_6_2", F.avg("monthly_fee").over(w7))
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "plan",
        "status",
        "end_date_day",
        "last4_sum_end_date_day",
        "last4_count_end_date_day",
        "last4_min_end_date_day",
        "end_date_period_stddev_samp_end_date_day",
    )
)
