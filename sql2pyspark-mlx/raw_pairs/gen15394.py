from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

prev_events = sessionized.withColumn("prev_paid_at", F.lag("paid_at").over(w3))

flagged_2 = (
    prev_events
    .withColumn("is_new_session_2", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized_payments = flagged_2.withColumn("burst_id", F.sum("is_new_session_2").over(w4))

cleaned = (
    sessionized_payments
    .filter(F.col("order_id").isNotNull())
    .withColumn("paid_at_hour", F.hour("paid_at"))
)

joined = (
    cleaned
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select(
        "a.*",
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("o.amount").alias("order_amount"),
    )
)

totals = (
    joined
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.min("amount").alias("min_amount"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("island_id")).otherwise(0)).alias("mobile_island_id"),
    )
    .filter(F.col("mobile_island_id") < 1000000)
)

result = (
    totals
    .select("activity_month", "mobile_rows")
    .orderBy(F.col("mobile_rows").asc_nulls_first())
)
