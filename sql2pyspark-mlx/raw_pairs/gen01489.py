from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("amount_tier").orderBy(F.col("failed_events").desc())

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.col("order_id").isin([11, 34]))
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("clean_status", F.trim("status"))
)

with_gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("amount") >= 1620, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        "order_id",
    )
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        F.max("paid_at").alias("last_paid_at"),
        F.max("paid_at").alias("ended_at"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
    )
    .withColumn("failed_events_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("failed_events").desc())
)
