from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")
w2 = Window.partitionBy("method").orderBy("paid_at", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("paid_at_hour").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w5 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") > F.to_timestamp(F.lit("2023-10-01 18:00:00"))) & F.col("order_id").isNotNull() & (F.col("method") == "cash"))
    .withColumn("is_failed", F.when(F.col("status") == "FAILED", 1).otherwise(0))
    .withColumn("paid_at_hour", F.hour("paid_at"))
    .withColumn("method_filled", F.coalesce("method", F.lit("UNKNOWN")))
)

enriched = (
    base
    .select(
        "*",
        F.max("paid_at_hour").over(w).alias("method_max_paid_at_hour"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(3).over(w3).alias("paid_at_hour_bucket"),
    )
)

flagged = (
    enriched
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w4) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("session_start_flag").over(w5))

result = (
    islands
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "is_failed",
        "seq_no",
        "session_start_flag",
        "session_id",
        F.when(F.col("method").rlike("^(bank_transfer|promptpay)$"), "EXTENDED").otherwise("GROUP_A").alias("method_zone"),
    )
)
