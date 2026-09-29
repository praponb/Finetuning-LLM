from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(-2, -1)
w_recent = Window.partitionBy("status").orderBy("paid_at", "id")
w_grp = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.orderBy("amount_avg_6", "id")
w6 = Window.orderBy("amount_share_2", "id")
w7 = Window.orderBy("amount_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("amount_share_2_bucket", "id")

enriched = (
    df_payments
    .filter(F.col("order_id").isin([24, 34, 36]))
    .select(
        "id",
        "method",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.avg("amount").over(w2).alias("amount_avg_2_prior"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share_2"),
        F.avg("amount").over(w_grp).alias("amount_avg_6"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "amount_share",
        "amount_avg_2_prior",
        "seq_no",
        "amount_share_2",
        "amount_avg_6",
        F.row_number().over(w5).alias("event_seq"),
        F.ntile(10).over(w6).alias("amount_share_2_bucket"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "amount_avg_2_prior",
        "amount_share_2",
        "amount_avg_6",
        "event_seq",
        F.max("seq_no").over(w7).alias("running_max_seq_no"),
        F.avg("seq_no").over(w7).alias("running_avg_seq_no"),
        F.sum("seq_no").over(w7).alias("running_sum_seq_no"),
        F.lag("amount_avg_6", 1).over(w_hist).alias("prev_amount_avg_6"),
    )
)

valid = (
    with_windows
    .filter(F.col("running_sum_seq_no").between(15, 35))
    .select(
        "id",
        "amount_avg_2_prior",
        "amount_avg_6",
        "event_seq",
        "running_max_seq_no",
        "running_avg_seq_no",
        "running_sum_seq_no",
        "prev_amount_avg_6",
        F.least("event_seq", "amount_share_2").alias("least_event_seq_amount_share_2"),
    )
)

result = (
    valid
    .select(
        "id",
        "amount_avg_2_prior",
        "amount_avg_6",
        "event_seq",
        "running_max_seq_no",
        "running_sum_seq_no",
        "prev_amount_avg_6",
        "least_event_seq_amount_share_2",
    )
)
