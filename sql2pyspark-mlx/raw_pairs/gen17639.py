from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "dept_id").orderBy(F.col("hire_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("job_title").orderBy("hire_date", "id")
w_running = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("event_seq").desc(), "id")
w6 = Window.orderBy(F.col("seq_no").desc(), "id")
w7 = Window.orderBy("seq_no", "id")
w8 = Window.orderBy("event_seq_bucket", "id").rowsBetween(-2, Window.currentRow)
w9 = Window.orderBy("event_seq_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("event_seq_bucket", "id").rowsBetween(-5, -1)
w11 = Window.orderBy(F.col("cumulative_event_seq_bucket").desc_nulls_first())
w12 = Window.orderBy(F.col("event_seq_bucket_avg_5_prior").desc_nulls_first())
w13 = Window.orderBy("cumulative_event_seq_bucket_cume_dist", "id")
w14 = Window.orderBy("cumulative_event_seq_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_employees
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    ranked
    .select(
        "id",
        "full_name",
        "email",
        "salary",
        "hire_date",
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "email",
        "seq_no",
        F.sum("event_seq").over(w_running).alias("running_event_seq"),
        F.ntile(5).over(w5).alias("event_seq_bucket"),
    )
)

metrics_employees = (
    scored
    .withColumn("seq_no_bucket", F.ntile(4).over(w6))
    .withColumn("seq_no_2", F.row_number().over(w7))
    .withColumn("event_seq_bucket_avg_2", F.avg("event_seq_bucket").over(w8))
    .withColumn("cumulative_event_seq_bucket", F.sum("event_seq_bucket").over(w9))
    .withColumn("event_seq_bucket_avg_5_prior", F.avg("event_seq_bucket").over(w10))
)

scored_employees = (
    metrics_employees
    .select(
        "id",
        "seq_no",
        "running_event_seq",
        "seq_no_2",
        "event_seq_bucket_avg_2",
        "cumulative_event_seq_bucket",
        "event_seq_bucket_avg_5_prior",
        F.cume_dist().over(w11).alias("cumulative_event_seq_bucket_cume_dist"),
        F.percent_rank().over(w12).alias("event_seq_bucket_avg_5_prior_percent_rank"),
    )
)

result = (
    scored_employees
    .select(
        "id",
        "cumulative_event_seq_bucket",
        "event_seq_bucket_avg_5_prior",
        "cumulative_event_seq_bucket_cume_dist",
        "event_seq_bucket_avg_5_prior_percent_rank",
        F.lead("running_event_seq").over(w13).alias("next_running_event_seq"),
        F.lead("event_seq_bucket_avg_2").over(w13).alias("next_event_seq_bucket_avg_2"),
        F.row_number().over(w13).alias("event_seq"),
        F.sum("running_event_seq").over(w14).alias("cumulative_running_event_seq"),
    )
)
