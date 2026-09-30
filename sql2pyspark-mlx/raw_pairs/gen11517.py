from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date_quarter").desc_nulls_first(), "id")
w2 = Window.orderBy(F.col("monthly_fee").asc_nulls_last())
w3 = Window.partitionBy(F.date_trunc("YEAR", "start_date"), "plan").orderBy("end_date_quarter", "id")
w4 = Window.orderBy("days_start_date_to_end_date", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(-5, -1)
w_part = Window.partitionBy("customer_id", "plan")
w7 = Window.partitionBy("plan").orderBy("end_date", "id")

valid = (
    df_subscriptions
    .filter(F.col("start_date") >= F.lit("2023-06-15").cast("date"))
    .withColumn("days_start_date_to_end_date", F.datediff("end_date", "start_date"))
    .withColumn("end_date_quarter", F.quarter("end_date"))
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

cleaned = (
    latest
    .filter(F.col("customer_id").isNotNull() & F.col("days_start_date_to_end_date").between(23, 33))
    .select(
        "*",
        F.when(F.col("monthly_fee") >= 203, "High").when(F.col("monthly_fee") >= 98, "Medium").otherwise("Low").alias("monthly_fee_band"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("end_date_quarter") == 52)
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        F.cume_dist().over(w2).alias("monthly_fee_cume_dist"),
        F.lead("monthly_fee", 2).over(w3).alias("next_monthly_fee"),
        F.avg("days_start_date_to_end_date").over(w4).alias("days_start_date_to_end_date_avg_3"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee_cume_dist",
        "days_start_date_to_end_date_avg_3",
        F.avg("next_monthly_fee").over(w5).alias("next_monthly_fee_avg_5_prior"),
        F.count("monthly_fee_cume_dist").over(w_part).alias("customer_id_count_monthly_fee_cume_dist"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

totals = (
    scored
    .groupBy("plan", "customer_id")
    .agg(
        F.round(F.sum("monthly_fee_cume_dist") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("monthly_fee_cume_dist_per_basic"),
        F.avg(F.when(F.col("plan") == "basic", F.col("row_seq"))).alias("basic_avg_row_seq"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee_cume_dist"))).alias("enterprise_total_monthly_fee_cume_dist"),
    )
    .filter(F.col("basic_avg_row_seq") > 0)
)

result = (
    totals
    .select(
        "plan",
        "customer_id",
        "monthly_fee_cume_dist_per_basic",
        "basic_avg_row_seq",
    )
    .orderBy("monthly_fee_cume_dist_per_basic")
)
