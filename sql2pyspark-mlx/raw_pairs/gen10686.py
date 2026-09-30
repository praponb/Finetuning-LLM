from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").desc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")
w3 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("amount_rounded").desc_nulls_last())
w5 = Window.partitionBy("quarter_start").orderBy(F.col("distinct_ids").desc())

latest = (
    df_payments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "amount", "paid_at", "status")
)

filtered = (
    latest
    .filter(F.hour("paid_at").between(6, 14))
    .withColumn("amount_rounded", F.round("amount", 2))
    .withColumn("amount_band", F.when(F.col("amount") >= 1310, "High").when(F.col("amount") >= 250, "Medium").otherwise("Low"))
)

with_gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "paid_at",
        "status",
        "amount_rounded",
        "amount_band",
        F.sum("is_new_burst").over(w3).alias("session_id"),
    )
)

gaps = (
    islands
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w2) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_id_2", F.sum("is_new_burst").over(w3))

latest_payments = (
    sessions_payments
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select("id", "paid_at", "amount_rounded", "amount_band", "session_id")
)

result = (
    latest_payments
    .groupBy(
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
        F.when(F.col("session_id") >= 8, "HIGH").otherwise("LOW").alias("session_id_tier"),
    )
    .agg(
        F.min("paid_at").alias("first_seen"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .withColumn("distinct_ids_percent_rank", F.percent_rank().over(w5))
    .orderBy("quarter_start")
)
