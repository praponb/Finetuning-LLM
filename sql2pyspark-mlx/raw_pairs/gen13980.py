from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w6 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("status").orderBy("is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("method")
w9 = Window.partitionBy("method").orderBy("session_no", "id")

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-11-05 17:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .withColumn("status_group", F.when(F.col("status").isin(["SUCCESS", "REFUNDED"]), "Core").otherwise("Other"))
)

with_gaps = (
    base
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

flagged = (
    islands
    .withColumn("is_new_session_2", F.when(F.col("paid_at") - F.lag("paid_at").over(w3) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands_payments = flagged.withColumn("session_id", F.sum("is_new_session_2").over(w4))

gaps = (
    islands_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w5).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w5) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "status_group",
        "is_new_session",
        F.sum("is_new_burst").over(w6).alias("session_no"),
    )
)

scored = (
    sessionized
    .select(
        "id",
        "method",
        "amount",
        "status",
        "status_group",
        "is_new_session",
        "session_no",
        F.avg("amount").over(w_part).alias("running_avg_amount"),
        F.min("amount").over(w_part).alias("running_min_amount"),
        F.max("amount").over(w_part).alias("running_max_amount"),
        F.count("amount").over(w_part).alias("running_count_amount"),
        F.avg("amount").over(w_ordered).alias("method_avg_amount"),
    )
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "amount",
        "status",
        "session_no",
        "running_avg_amount",
        "running_min_amount",
        "running_max_amount",
        "running_count_amount",
        "rank_in_group",
    )
    .orderBy(F.col("running_count_amount").desc_nulls_last())
)
