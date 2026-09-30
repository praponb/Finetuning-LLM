from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("method").isin(["card", "bank_transfer"]))
    .select(
        "*",
        F.trim("status").alias("clean_status"),
        F.coalesce("method", F.lit("N/A")).alias("method_clean"),
    )
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
        F.date_trunc("YEAR", "paid_at").alias("year_start"),
    )
    .agg(
        F.max("paid_at").alias("last_seen"),
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
    )
)
