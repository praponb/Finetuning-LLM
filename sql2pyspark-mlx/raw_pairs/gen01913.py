from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("months_start_date_to_end_date").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.month("end_date"), "status").orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("plan", "customer_id").orderBy("monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("end_date").desc_nulls_first())
w5 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w_part = Window.partitionBy(F.date_trunc("week", "end_date"), "status").orderBy("months_start_date_to_end_date_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status")
w8 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("end_date") >= F.lit("2023-05-29").cast("date")))
    .withColumn("months_start_date_to_end_date", F.round((F.datediff("end_date", "start_date") / 30), 1))
    .withColumn("months_start_date_to_end_date_2", F.round((F.datediff("end_date", "start_date") / 30), 1))
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    latest
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "status",
        "months_start_date_to_end_date",
        F.avg("months_start_date_to_end_date").over(w2).alias("months_start_date_to_end_date_avg_6"),
        F.avg("months_start_date_to_end_date_2").over(w3).alias("months_start_date_to_end_date_2_avg_3"),
    )
)

latest_subscriptions = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

base_subscriptions = (
    latest_subscriptions
    .filter(F.col("end_date") > F.to_date(F.lit("2023-06-15")))
    .withColumn("monthly_fee_minus_months_start_date_to_end_date", (F.col("monthly_fee") - F.col("months_start_date_to_end_date")))
    .withColumn("months_start_date_to_end_date_2_avg_3_times_months_start_date_to_end_date_avg_6", (F.col("months_start_date_to_end_date_2_avg_3") * F.col("months_start_date_to_end_date_avg_6")))
)

ranked = (
    base_subscriptions
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    ranked
    .filter(F.col("months_start_date_to_end_date_2_avg_3") == 32)
    .select(
        "id",
        "monthly_fee",
        "status",
        "months_start_date_to_end_date",
        "months_start_date_to_end_date_avg_6",
        "months_start_date_to_end_date_2_avg_3",
        "monthly_fee_minus_months_start_date_to_end_date",
        "months_start_date_to_end_date_2_avg_3_times_months_start_date_to_end_date_avg_6",
        F.first("monthly_fee", ignorenulls=True).over(w_part).alias("first_known_monthly_fee"),
        F.stddev_samp("months_start_date_to_end_date_2_avg_3").over(w7).alias("status_stddev_samp_months_start_date_to_end_date_2_avg_3"),
        F.last("monthly_fee", ignorenulls=True).over(w8).alias("last_known_monthly_fee"),
    )
)

result = (
    metrics
    .select(
        "id",
        "monthly_fee",
        "months_start_date_to_end_date_avg_6",
        "months_start_date_to_end_date_2_avg_3_times_months_start_date_to_end_date_avg_6",
    )
    .orderBy("id")
)
