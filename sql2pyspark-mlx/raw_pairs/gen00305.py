from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("paid_at", "id")
w4 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method", "status").orderBy("burst_id", "id")
w6 = Window.partitionBy(F.date_trunc("WEEK", "paid_at"), "status").orderBy("burst_id", "id").rowsBetween(-6, Window.currentRow)
w_part = Window.partitionBy("method").orderBy("next_is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("method").orderBy("burst_id_whole", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.orderBy("amount", "id")

valid = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-10-13").cast("date")) & F.col("method").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.trim("method").alias("clean_method"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

flagged = (
    sessionized
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w3) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized_payments = flagged.withColumn("burst_id", F.sum("is_new_session").over(w4))

base = (
    sessionized_payments
    .filter((F.col("paid_at") >= F.lit("2023-10-21 04:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .withColumn("burst_id_whole", F.ceil("burst_id"))
)

enriched = (
    base
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "session_no",
        "is_new_session",
        "burst_id_whole",
        F.lag("is_new_session").over(w5).alias("prev_is_new_session"),
        F.lead("is_new_session").over(w5).alias("next_is_new_session"),
        F.lag("amount").over(w5).alias("prev_amount"),
        F.row_number().over(w5).alias("event_seq"),
        F.avg("burst_id").over(w6).alias("burst_id_avg_6"),
    )
)

metrics = (
    enriched
    .filter(F.col("burst_id_avg_6") != 7)
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "is_new_session",
        "prev_is_new_session",
        "next_is_new_session",
        "prev_amount",
        "burst_id_avg_6",
        F.first("burst_id_whole", ignorenulls=True).over(w_part).alias("first_known_burst_id_whole"),
        F.avg("prev_amount").over(w8).alias("prev_amount_avg_5"),
        F.row_number().over(w9).alias("row_seq"),
    )
)

result = (
    metrics
    .groupBy("method")
    .agg(
        F.count_if(F.col("method") == "cash").alias("cash_events"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.round(F.avg("prev_is_new_session"), 2).alias("avg_prev_is_new_session"),
        F.round(F.avg("burst_id_avg_6"), 2).alias("avg_burst_id_avg_6"),
    )
    .orderBy(F.col("method").desc())
)
