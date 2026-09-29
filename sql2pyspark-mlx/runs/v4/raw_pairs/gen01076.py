from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method", "status").orderBy("prev_paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("prev_paid_at", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

with_prev = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

scored = (
    islands
    .select(
        "*",
        F.last("method", ignorenulls=True).over(w3).alias("last_known_method"),
        F.avg("is_new_session").over(w4).alias("is_new_session_avg_3"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

wide = (
    ranked
    .groupBy("method", "order_id")
    .pivot("status", ["SUCCESS", "REFUNDED"])
    .agg(
        F.max("is_new_session_avg_3").alias("amount"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "method",
        "order_id",
        F.col("SUCCESS_amount").alias("success_amount"),
        F.col("SUCCESS_item_count").alias("success_item_count"),
        F.col("REFUNDED_amount").alias("refunded_amount"),
        F.col("REFUNDED_item_count").alias("refunded_item_count"),
    )
)

result = (
    wide
    .select(
        "method",
        "order_id",
        "success_amount",
        "success_item_count",
        "refunded_amount",
        "refunded_item_count",
    )
    .orderBy("order_id", "refunded_amount")
)
