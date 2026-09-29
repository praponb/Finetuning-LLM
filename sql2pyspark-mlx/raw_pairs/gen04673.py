from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("days_end_date_to_start_date").asc_nulls_first(), "id")
w_recent = Window.partitionBy("customer_id").orderBy(F.col("days_end_date_to_start_date").desc())
w3 = Window.partitionBy(F.date_trunc("MONTH", "start_date"), "customer_id").orderBy(F.col("days_end_date_to_start_date").desc_nulls_first())
w4 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent_2 = Window.partitionBy("customer_id").orderBy("start_date", "id")
w6 = Window.partitionBy("status", "plan").orderBy("days_end_date_to_start_date", "id").rowsBetween(-6, -1)
w7 = Window.orderBy("days_end_date_to_start_date_percent_rank", "id")
w8 = Window.partitionBy(F.year("start_date"), "plan").orderBy("days_end_date_to_start_date_rank", "id")
w9 = Window.partitionBy("status").orderBy("first_known_days_end_date_to_start_date", "id")
w10 = Window.partitionBy(F.year("end_date"), "status").orderBy("first_known_days_end_date_to_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy(F.year("start_date"), "customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.percent_rank().over(w_recent).alias("days_end_date_to_start_date_percent_rank"),
        F.rank().over(w3).alias("days_end_date_to_start_date_rank"),
        F.first("days_end_date_to_start_date", ignorenulls=True).over(w4).alias("first_known_days_end_date_to_start_date"),
        F.row_number().over(w_recent_2).alias("row_seq"),
        F.avg("days_end_date_to_start_date").over(w6).alias("days_end_date_to_start_date_avg_6_prior"),
    )
)

enriched = (
    metrics
    .withColumn("days_end_date_to_start_date_percent_rank_bucket", F.ntile(4).over(w7))
    .withColumn("next_days_end_date_to_start_date_avg_6_prior", F.lead("days_end_date_to_start_date_avg_6_prior", 2).over(w8))
)

metrics_subscriptions = (
    enriched
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "days_end_date_to_start_date",
        "row_num",
        "days_end_date_to_start_date_percent_rank",
        "days_end_date_to_start_date_rank",
        "days_end_date_to_start_date_avg_6_prior",
        "days_end_date_to_start_date_percent_rank_bucket",
        F.lead("days_end_date_to_start_date_rank", 2).over(w9).alias("next_days_end_date_to_start_date_rank"),
        F.sum("days_end_date_to_start_date_percent_rank").over(w10).alias("running_days_end_date_to_start_date_percent_rank"),
        F.sum("next_days_end_date_to_start_date_avg_6_prior").over(w11).alias("cumulative_next_days_end_date_to_start_date_avg_6_prior"),
    )
)

result = (
    metrics_subscriptions
    .select(
        "id",
        "end_date",
        "days_end_date_to_start_date_percent_rank",
        "days_end_date_to_start_date_rank",
        "days_end_date_to_start_date_percent_rank_bucket",
        "next_days_end_date_to_start_date_rank",
        "running_days_end_date_to_start_date_percent_rank",
        "cumulative_next_days_end_date_to_start_date_avg_6_prior",
    )
)
