from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.orderBy("amount", "id")
w3 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")
w5 = Window.partitionBy("status").orderBy("paid_at", "id")
w6 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_payments
    .filter(F.col("status") != "REFUNDED")
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_30d"),
        F.ntile(3).over(w2).alias("amount_bucket"),
        F.first("method", ignorenulls=True).over(w3).alias("first_known_method"),
    )
)

deduped = (
    with_windows
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "paid_at",
        "status",
        "amount_avg_30d",
        "amount_bucket",
        "first_known_method",
    )
)

flagged = (
    deduped
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w5) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_group_flag").over(w6))

with_gaps = (
    sessionized
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w5) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w6))

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("amount_avg_30d") >= 1305, "HIGH").otherwise("LOW").alias("amount_avg_30d_tier"),
        F.when(F.col("amount_bucket") >= 9, "PREMIUM").otherwise("STANDARD").alias("amount_bucket_tier"),
        F.date_trunc("year", "paid_at").alias("year_start"),
    )
    .agg(
        F.max(F.when(F.col("status") == "REFUNDED", F.col("session_no"))).alias("refunded_max_session_no"),
        F.countDistinct("status").alias("distinct_status"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("amount_avg_30d_tier").asc_nulls_first())
)
