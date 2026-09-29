from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("days_since_end_date").desc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy("days_since_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("days_since_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("plan", "status").orderBy(F.col("days_since_end_date").asc_nulls_last(), "id")
w_ordered = Window.partitionBy("status").orderBy("monthly_fee", "id")

base = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-06-07").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.dense_rank().over(w).alias("days_since_end_date_dense_rank"),
        F.last("days_since_end_date", ignorenulls=True).over(w2).alias("last_known_days_since_end_date"),
        F.sum("monthly_fee").over(w3).alias("cumulative_monthly_fee"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "status",
        "days_since_end_date",
        "last_known_days_since_end_date",
        "cumulative_monthly_fee",
    )
)

with_windows = (
    latest
    .select(
        "*",
        F.row_number().over(w_ordered).alias("seq_no"),
        F.ntile(5).over(w_ordered).alias("monthly_fee_bucket"),
    )
)

result = (
    with_windows
    .select("id", "status", "seq_no")
    .orderBy("seq_no")
)
