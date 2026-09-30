from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("paid_at"), "order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("method", "status").orderBy("amount", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w4 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.orderBy("next_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy(F.year("paid_at")).orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.orderBy(F.col("amount_avg_3_prior").desc(), "id")
w8 = Window.partitionBy(F.date_trunc("MONTH", "paid_at")).orderBy("amount_avg_3_prior_sum_7d", "id")
w9 = Window.orderBy("cumulative_amount", "id")
w_running = Window.orderBy("amount_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select("*", F.upper("status").alias("status_upper"))
)

with_windows = (
    filtered
    .filter(F.col("order_id").isin([7, 9, 13, 30]))
    .select(
        "id",
        "amount",
        "paid_at",
        F.lead("amount", 2, 0).over(w).alias("next_amount"),
        F.avg("amount").over(w2).alias("amount_avg_3_prior"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

with_windows_payments = (
    with_windows
    .withColumn("amount_avg_3_prior_sum_7d", F.sum("amount_avg_3_prior").over(w4))
    .withColumn("cumulative_amount", F.sum("amount").over(w5))
    .withColumn("amount_avg_3_prior_max_7d", F.max("amount_avg_3_prior").over(w_part))
)

enriched = (
    with_windows_payments
    .select(
        "*",
        F.ntile(10).over(w7).alias("amount_avg_3_prior_bucket"),
        F.lag("cumulative_amount", 2).over(w8).alias("prev_cumulative_amount"),
        F.row_number().over(w9).alias("row_seq"),
    )
)

enriched_payments = (
    enriched
    .select(
        "id",
        "paid_at",
        "amount_avg_3_prior",
        "cumulative_amount",
        "amount_avg_3_prior_max_7d",
        "prev_cumulative_amount",
        F.sum("seq_no").over(w_running).alias("running_sum_seq_no"),
        F.max("seq_no").over(w_running).alias("running_max_seq_no"),
        F.min("seq_no").over(w_running).alias("running_min_seq_no"),
        F.count("seq_no").over(w_running).alias("running_count_seq_no"),
    )
)

result = (
    enriched_payments
    .select(
        "id",
        "amount_avg_3_prior",
        "amount_avg_3_prior_max_7d",
        "prev_cumulative_amount",
        "running_sum_seq_no",
        "running_min_seq_no",
        "running_count_seq_no",
    )
    .orderBy(
        F.col("amount_avg_3_prior_max_7d").desc_nulls_last(),
        F.col("running_min_seq_no").desc(),
    )
)
