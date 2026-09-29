from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-04-20").cast("date")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("amount") >= 1250, "High").when(F.col("amount") >= 760, "Medium").otherwise("Low").alias("amount_tier"),
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
    )
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "amount_tier",
        "method_filled",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_payments
    .groupBy(F.date_trunc("WEEK", "paid_at").alias("week_start"), "order_id", "method")
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.sum(F.when(F.col("method") == "paypal", F.col("amount")).otherwise(0)).alias("paypal_amount"),
        F.sum(F.when(F.col("method") == "cash", F.col("burst_id")).otherwise(0)).alias("cash_burst_id"),
    )
)
