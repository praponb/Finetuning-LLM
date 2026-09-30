from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("start_date_plus_90d").desc(), "id")
w3 = Window.partitionBy("plan").orderBy(F.col("end_date").desc(), "id")
w4 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy(F.year("start_date_plus_90d"), "plan")
w_running = Window.partitionBy("plan").orderBy("seq_no", "id")

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.date_add("start_date", 90).alias("start_date_plus_90d"),
    )
)

scored = base.withColumn("row_seq", F.row_number().over(w))

scored_subscriptions = (
    scored
    .select(
        "id",
        "plan",
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("seq_no_2"),
        F.avg("row_seq").over(w4).alias("row_seq_avg_5_prior"),
        F.sum("row_seq").over(w5).alias("start_date_plus_90d_year_sum_row_seq"),
    )
)

metrics = (
    scored_subscriptions
    .select(
        "*",
        F.lead("seq_no_2").over(w_running).alias("next_seq_no_2"),
        F.lag("seq_no").over(w_running).alias("prev_seq_no"),
        F.lead("seq_no").over(w_running).alias("next_seq_no"),
        F.row_number().over(w_running).alias("seq_no_3"),
    )
)

result = (
    metrics
    .select(
        "id",
        "plan",
        "seq_no",
        "seq_no_2",
        "row_seq_avg_5_prior",
        "start_date_plus_90d_year_sum_row_seq",
        "prev_seq_no",
        "next_seq_no",
    )
    .orderBy(F.col("seq_no_2").desc_nulls_last())
)
