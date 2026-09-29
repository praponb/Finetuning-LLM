from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("method").orderBy(F.col("paid_at").desc(), "id")

metrics = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "cash"]))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.percent_rank().over(w).alias("amount_percent_rank"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

result = (
    metrics
    .groupBy("order_id", "method")
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.sum("event_seq").alias("total_event_seq"),
        F.max("paid_at").alias("last_paid_at"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("amount_percent_rank")).otherwise(0)).alias("promptpay_amount_percent_rank"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
    )
    .orderBy(F.col("total_event_seq").asc_nulls_first())
)
