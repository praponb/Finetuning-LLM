from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-09-05"))) & F.col("order_id").isNotNull())
    .withColumn("paid_at_quarter", F.quarter("paid_at"))
)

with_gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_payments
    .groupBy("burst_id")
    .agg(
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
        F.max("amount").alias("max_amount"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
    )
    .orderBy(F.col("promptpay_count").desc_nulls_first())
)
