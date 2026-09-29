from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("monthly_fee")
w3 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.date_trunc("MONTH", "start_date"), "customer_id").orderBy("end_date", "id")
w5 = Window.partitionBy("plan").orderBy(F.col("running_monthly_fee").desc(), "id")
w6 = Window.orderBy(F.col("running_monthly_fee_2").desc(), "id")
w7 = Window.partitionBy("plan")
w8 = Window.partitionBy("customer_id", "plan").orderBy(F.col("running_monthly_fee_2").asc_nulls_last(), "id")
w9 = Window.partitionBy("plan").orderBy(F.col("running_monthly_fee").desc_nulls_last(), "id")

with_windows = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.percent_rank().over(w2).alias("monthly_fee_percent_rank"),
        F.sum("monthly_fee").over(w3).alias("running_monthly_fee_2"),
        F.lag("monthly_fee", 1, 0).over(w4).alias("prev_monthly_fee"),
    )
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.age"), F.col("d.country"), F.col("d.email"))
)

with_windows_subscriptions = (
    with_dims
    .withColumn("running_monthly_fee_bucket", F.ntile(3).over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("plan_count_running_monthly_fee_2", F.count("running_monthly_fee_2").over(w7))
)

latest = (
    with_windows_subscriptions
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "plan",
        "running_monthly_fee",
        "monthly_fee_percent_rank",
        "running_monthly_fee_2",
        "prev_monthly_fee",
        "email",
    )
)

deduped = (
    latest
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("prev_monthly_fee") >= 197, "PREMIUM").otherwise("STANDARD").alias("prev_monthly_fee_tier"),
    )
    .agg(
        F.round(F.avg("running_monthly_fee_2"), 2).alias("avg_running_monthly_fee_2"),
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee_percent_rank")).otherwise(0)).alias("premium_monthly_fee_percent_rank"),
    )
    .filter(F.col("avg_running_monthly_fee_2") > 0)
)
