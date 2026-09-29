from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method").orderBy("paid_at_period", "id")
w3 = Window.partitionBy("method").orderBy("paid_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-06-18"))) & F.col("method").isNotNull())
    .select(
        "*",
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
        F.coalesce("method", F.lit("N/A")).alias("method_filled_2"),
    )
)

filtered = (
    base
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "method_filled",
        "method_filled_2",
        F.date_trunc("month", "paid_at").alias("paid_at_period"),
    )
)

metrics = (
    filtered
    .select("*", F.last("method", ignorenulls=True).over(w).alias("last_known_method"))
)

with_prev = metrics.withColumn("prev_paid_at_period", F.lag("paid_at_period").over(w2))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at_period").isNull() | (F.col("paid_at_period") > F.col("prev_paid_at_period") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w3))

result = (
    islands
    .select(
        "id",
        "method",
        "method_filled",
        "method_filled_2",
        "last_known_method",
        "is_new_burst",
        "session_no",
    )
    .orderBy(F.col("id").desc_nulls_first(), "is_new_burst")
)
