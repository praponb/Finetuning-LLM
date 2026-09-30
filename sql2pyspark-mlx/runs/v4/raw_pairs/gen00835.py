from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("adjusted_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("order_id").orderBy("amount", "id")
w3 = Window.partitionBy("status").orderBy(F.col("adjusted_amount").desc())
w4 = Window.partitionBy("status").orderBy(F.col("paid_at").desc(), "id")
w5 = Window.partitionBy("order_id").orderBy("prev_adjusted_amount", "id")

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2024-03-25 04:00:00").cast("timestamp")) & F.col("method").isNotNull() & (F.col("amount") <= 410))
    .withColumn("clean_status", F.trim("status"))
    .withColumn("adjusted_amount", F.round((F.col("amount") * 0.85), 2))
    .withColumn("method_clean", F.coalesce("method", F.lit("OTHER")))
)

metrics = (
    valid
    .withColumn("cumulative_amount", F.sum("amount").over(w))
    .withColumn("prev_adjusted_amount", F.lag("adjusted_amount", 2).over(w_recent))
)

metrics_payments = (
    metrics
    .select(
        "*",
        F.percent_rank().over(w3).alias("adjusted_amount_percent_rank"),
        F.row_number().over(w4).alias("event_seq"),
        F.lag("amount", 1, 0).over(w5).alias("prev_amount"),
    )
)

base = (
    metrics_payments
    .filter(F.col("event_seq") <= 4)
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "clean_status",
        "adjusted_amount",
        "method_clean",
        "prev_adjusted_amount",
        "adjusted_amount_percent_rank",
        "event_seq",
        F.ceil("adjusted_amount_percent_rank").alias("adjusted_amount_percent_rank_whole"),
        F.when(F.col("method") == "card", 1).otherwise(0).alias("is_card"),
    )
)

result = (
    base
    .select(
        "id",
        "order_id",
        "adjusted_amount",
        "prev_adjusted_amount",
        "adjusted_amount_percent_rank",
        "amount",
    )
    .unpivot(
        ["id", "order_id"],
        ["adjusted_amount", "prev_adjusted_amount", "adjusted_amount_percent_rank", "amount"],
        "kpi",
        "val",
    )
    .filter(F.col("val").isNotNull())
)
