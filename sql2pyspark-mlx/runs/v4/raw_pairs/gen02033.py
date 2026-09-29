from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method", "order_id").orderBy("is_new_burst", "id")
w4 = Window.partitionBy("order_id", "status").orderBy(F.col("session_no").desc(), "id")
w5 = Window.partitionBy("status").orderBy(F.col("is_new_burst").asc_nulls_first())
w6 = Window.orderBy("paid_at", "id")
w7 = Window.orderBy(F.col("paid_at").desc(), "id")
w8 = Window.partitionBy("order_id", "method").orderBy(F.col("is_new_burst_rank").desc_nulls_first(), "id")
w9 = Window.orderBy("amount", "id").rowsBetween(-3, -1)
w10 = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

metrics = (
    islands
    .select(
        "*",
        F.ntile(3).over(w3).alias("is_new_burst_bucket"),
        F.row_number().over(w4).alias("event_seq"),
        F.rank().over(w5).alias("is_new_burst_rank"),
        F.row_number().over(w6).alias("row_seq"),
        F.row_number().over(w7).alias("row_seq_2"),
    )
)

ranked = (
    metrics
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.avg("row_seq").over(w9).alias("row_seq_avg_3_prior"),
        F.sum("row_seq").over(w10).alias("running_row_seq"),
        F.max("is_new_burst").over(w11).alias("is_new_burst_max_7d"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "is_new_burst",
        "session_no",
        "is_new_burst_rank",
        "row_seq",
        "row_seq_2",
        F.when(F.col("status").rlike("^(REFUNDED|FAILED)$"), "EXTENDED").when(F.col("status").rlike("^(SUCCESS)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("is_new_burst_rank").asc_nulls_first())
)
