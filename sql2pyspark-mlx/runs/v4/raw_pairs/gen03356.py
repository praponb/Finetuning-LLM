from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("method_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method", "status")
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-12-17 23:00:00").cast("timestamp"))
    .withColumn("method_length", F.length("method"))
)

cleaned = (
    filtered
    .filter(F.col("paid_at") > F.lit("2023-10-10").cast("date"))
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "method_length",
        (F.col("method_length") - F.col("amount")).alias("method_length_minus_amount"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean"),
    )
)

enriched = (
    cleaned
    .withColumn("running_method_length_minus_amount", F.sum("method_length_minus_amount").over(w))
    .withColumn("method_avg_method_length_minus_amount", F.avg("method_length_minus_amount").over(w2))
)

with_gaps = (
    enriched
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w3) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w4))

result = (
    sessions_payments
    .select(
        "id",
        "method",
        "paid_at",
        "method_length_minus_amount",
        "running_method_length_minus_amount",
        "session_start_flag",
        "session_id",
        F.when(F.col("method").rlike("^(paypal|bank_transfer)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("method_zone"),
    )
    .orderBy("session_id")
)
