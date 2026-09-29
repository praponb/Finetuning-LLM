from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-10-22").cast("date")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        F.lower("status").alias("status_lower"),
        F.hour("paid_at").alias("paid_at_hour"),
    )
)

with_gaps = (
    base
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("QUARTER", "paid_at").alias("quarter_start"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
        F.percentile_approx("session_no", 0.5).alias("session_no_p50"),
        F.min("paid_at").alias("first_seen"),
    )
    .filter(F.col("avg_amount") >= 0)
)
