from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-07-30").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("paid_at_label", F.date_format("paid_at", "MMM yyyy"))
)

flagged = (
    cleaned
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessions_payments
    .groupBy(
        F.date_trunc("month", "paid_at").alias("activity_month"),
        F.when(F.col("burst_id") >= 9, "HIGH").otherwise("LOW").alias("burst_id_tier"),
    )
    .agg(
        F.count_if(F.col("method") == "paypal").alias("paypal_events"),
        F.min("paid_at").alias("first_seen"),
        F.size(F.collect_set("method")).alias("num_methods"),
    )
    .filter(F.col("num_methods") > 1)
    .orderBy(F.col("num_methods").desc_nulls_last())
)
