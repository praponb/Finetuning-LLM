from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("paid_at", "id")
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy(F.date_trunc("YEAR", "order_date"), "order_id").orderBy("order_date", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("status").orderBy(F.col("order_date").desc(), "id")
w6 = Window.partitionBy("order_status", "status").orderBy("order_date", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("order_status")
w8 = Window.partitionBy(F.year("order_date"), "order_status").orderBy("prev_amount_2")
w9 = Window.partitionBy("order_status").orderBy(F.col("amount").desc(), "id")
w10 = Window.partitionBy("status", "order_status").orderBy(F.col("prev_amount_2").desc_nulls_first(), "id")

with_dims = (
    df_payments
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .filter(F.col("p.shipping_fee") <= 26)
    .select(
        "src.id",
        "src.order_id",
        "src.amount",
        "src.paid_at",
        "src.status",
        F.col("p.status").alias("order_status"),
        F.col("p.order_date"),
    )
)

with_windows = (
    with_dims
    .select(
        "id",
        "amount",
        "status",
        "order_status",
        "order_date",
        F.sum("amount").over(w).alias("running_amount"),
        F.lag("amount", 1, 0).over(w2).alias("prev_amount"),
        F.lag("amount", 2).over(w3).alias("prev_amount_2"),
        F.avg("amount").over(w4).alias("amount_avg_3_prior"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

with_windows_payments = (
    with_windows
    .withColumn("prev_amount_avg_6", F.avg("prev_amount").over(w6))
    .withColumn("amount_avg_3_prior_share", F.round((F.col("amount_avg_3_prior") / F.sum("amount_avg_3_prior").over(w7)), 4))
    .withColumn("prev_amount_2_cume_dist", F.cume_dist().over(w8))
)

ranked = (
    with_windows_payments
    .withColumn("dedup_rank", F.row_number().over(w9))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "status",
        "order_status",
        "prev_amount",
        "prev_amount_2",
        "amount_avg_3_prior",
        "seq_no",
        "amount_avg_3_prior_share",
    )
)

latest = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w10))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .rollup(
        "seq_no",
        "order_status",
        F.when(F.col("seq_no") >= 8, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
    )
    .agg(
        F.percentile_approx("amount_avg_3_prior_share", 0.5).alias("amount_avg_3_prior_share_p50"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        F.count(F.when(F.col("order_status") == "COMPLETED", 1)).alias("completed_rows"),
    )
    .filter(F.col("completed_rows") < 1000)
)
