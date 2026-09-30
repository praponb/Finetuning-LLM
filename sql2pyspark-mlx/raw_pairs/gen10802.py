from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id", "order_status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-11-07 19:00:00").cast("timestamp")) & (F.col("amount") <= 1050))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("status_method", F.concat("status", F.lit(" - "), "method"))
)

base_payments = (
    base
    .filter(F.col("status") == "FAILED")
    .withColumn("method_filled", F.coalesce("method", F.lit("N/A")))
    .withColumn("resolved_method", F.coalesce("method", F.lit("N/A")))
)

enriched = (
    base_payments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.store_id").isin([11, 31]))
    .select(
        "src.id",
        "src.order_id",
        "src.paid_at",
        "src.amount_filled",
        "src.status_method",
        "src.method_filled",
        "src.resolved_method",
        F.col("d.status").alias("order_status"),
        F.col("d.order_date"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.max("amount_filled").over(w_grp).alias("running_max_amount_filled"),
        F.min("amount_filled").over(w_grp).alias("running_min_amount_filled"),
        F.avg("amount_filled").over(w_grp).alias("running_avg_amount_filled"),
        F.sum("amount_filled").over(w2).alias("cumulative_amount_filled"),
    )
)

result = (
    metrics
    .groupBy(
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
    )
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.size(F.collect_set("order_status")).alias("num_order_status"),
        F.size(F.collect_set("order_status")).alias("num_order_status_2"),
    )
    .orderBy(F.col("num_order_status_2").desc())
)
