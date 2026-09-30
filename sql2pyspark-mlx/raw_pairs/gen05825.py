from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status").orderBy("paid_at_hour", "id")
w2 = Window.partitionBy(F.to_date("paid_at"), "status").orderBy("paid_at_hour")
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("is_new_session").desc())

base = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-10-16"))) & (~F.col("method").eqNullSafe(F.lit("cash"))))
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.lower("method").alias("method_lower"),
        F.hour("paid_at").alias("paid_at_hour"),
    )
)

enriched = (
    base
    .select(
        "id",
        "paid_at",
        "status",
        "method_lower",
        "paid_at_hour",
        F.lead("paid_at_hour").over(w_ordered).alias("next_paid_at_hour"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.rank().over(w2).alias("paid_at_hour_rank"),
    )
)

with_gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w3).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w4))

remaining_keys = (
    sessionized
    .select("status")
    .exceptAll(
        df_payments
        .filter(F.col("order_id").isin([9, 24, 28]))
        .select("status")
    )
)

verified = (
    sessionized
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("paid_at") >= F.lit("2023-08-07").cast("date"))
    .select("*", F.col("is_new_session").cast("double").alias("is_new_session_str"))
)

filtered_payments = (
    filtered
    .filter((F.col("paid_at") >= F.lit("2023-10-20 06:00:00").cast("timestamp")) & (F.col("seq_no") <= 3))
    .select(
        "*",
        F.when(F.col("session_id") >= 6, "High").when(F.col("session_id") >= 4, "Medium").otherwise("Low").alias("session_id_tier"),
    )
)

enriched_payments = (
    filtered_payments
    .select(
        "id",
        "paid_at",
        "status",
        "paid_at_hour",
        "next_paid_at_hour",
        "seq_no",
        "session_id",
        "session_id_tier",
        F.cume_dist().over(w5).alias("is_new_session_cume_dist"),
    )
)

result = (
    enriched_payments
    .select("id", "seq_no", "session_id", "session_id_tier", "is_new_session_cume_dist")
    .orderBy("is_new_session_cume_dist", "id")
)
