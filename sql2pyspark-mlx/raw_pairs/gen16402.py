from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy(F.col("start_date").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.date_trunc("YEAR", "start_date"), "customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("start_date", "id")
w4 = Window.partitionBy("plan", "customer_id").orderBy(F.col("monthly_fee_whole").desc_nulls_last())
w5 = Window.partitionBy("plan")
w6 = Window.partitionBy("plan").orderBy("latest_rank", "id")
w_hist = Window.orderBy("row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("row_seq_2", "id")
w_recent = Window.orderBy("running_max_row_seq_2", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.orderBy("running_count_row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .select(
        "id",
        "plan",
        "monthly_fee_whole",
        "latest_rank",
        F.first("monthly_fee_whole", ignorenulls=True).over(w2).alias("first_known_monthly_fee_whole"),
        F.row_number().over(w3).alias("row_seq"),
        F.cume_dist().over(w4).alias("monthly_fee_whole_cume_dist"),
    )
)

metrics = (
    enriched
    .filter(F.col("latest_rank") != 1)
    .select(
        "id",
        "latest_rank",
        F.round((F.col("row_seq") / F.sum("row_seq").over(w5)), 4).alias("row_seq_share"),
        F.row_number().over(w6).alias("row_seq_2"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "row_seq_2",
        F.sum("row_seq_2").over(w_hist).alias("running_sum_row_seq_2"),
        F.count("row_seq_2").over(w_hist).alias("running_count_row_seq_2"),
        F.max("row_seq_2").over(w_hist).alias("running_max_row_seq_2"),
        F.avg("row_seq_2").over(w_hist).alias("running_avg_row_seq_2"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "row_seq_2",
        "running_sum_row_seq_2",
        "running_count_row_seq_2",
        "row_seq",
        F.sum("running_max_row_seq_2").over(w_recent).alias("last3_sum_running_max_row_seq_2"),
        F.max("running_max_row_seq_2").over(w_recent).alias("last3_max_running_max_row_seq_2"),
        F.count("running_max_row_seq_2").over(w_recent).alias("last3_count_running_max_row_seq_2"),
        F.avg("running_max_row_seq_2").over(w_recent).alias("last3_avg_running_max_row_seq_2"),
        F.first("row_seq_2", ignorenulls=True).over(w_part).alias("first_known_row_seq_2"),
    )
)
