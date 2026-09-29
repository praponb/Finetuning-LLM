from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("days_since_paid_at").desc_nulls_last(), "id")
w_recent = Window.partitionBy("order_id", "status").orderBy("days_since_paid_at", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")
w_running = Window.orderBy("days_since_paid_at", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("paid_at", "id")
w_grp = Window.partitionBy("order_id", "status").orderBy("days_since_paid_at_avg_2", "id")
w8 = Window.partitionBy("order_id")

base = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-07-11").cast("date"))
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    latest
    .withColumn("days_since_paid_at_bucket", F.ntile(5).over(w_recent))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("days_since_paid_at_avg_2", F.avg("days_since_paid_at").over(w_running))
    .withColumn("first_known_days_since_paid_at", F.first("days_since_paid_at", ignorenulls=True).over(w_ordered))
    .withColumn("prev_days_since_paid_at", F.lag("days_since_paid_at", 2, 0).over(w6))
)

metrics_payments = (
    metrics
    .withColumn("prev_days_since_paid_at_bucket", F.lag("days_since_paid_at_bucket", 2).over(w_grp))
    .withColumn("order_id_avg_first_known_days_since_paid_at", F.avg("first_known_days_since_paid_at").over(w8))
)

valid = (
    metrics_payments
    .filter(F.col("order_id").isNotNull() & (F.col("first_known_days_since_paid_at") <= 66))
    .withColumn("prev_days_since_paid_at_minus_days_since_paid_at", (F.col("prev_days_since_paid_at") - F.col("days_since_paid_at")))
)

result = (
    valid
    .groupBy("order_id")
    .pivot("status", ["FAILED", "REFUNDED"])
    .agg(
        F.sum("days_since_paid_at_bucket").alias("total"),
        F.count(F.lit(1)).alias("cnt"),
    )
)
