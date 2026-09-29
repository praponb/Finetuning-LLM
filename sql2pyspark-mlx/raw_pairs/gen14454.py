from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("island_id", "id")
w4 = Window.partitionBy("order_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("YEAR", "paid_at"), "order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("session_start_flag", "id")

filtered = (
    df_payments
    .filter(F.to_date("paid_at") < "2024-10-03")
    .withColumn("months_since_paid_at", (F.datediff(F.current_date(), "paid_at") / 30))
    .withColumn("method_filled", F.coalesce("method", F.lit("DIRECT")))
)

gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "order_id",
        "paid_at",
        "months_since_paid_at",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

deduped = (
    sessionized
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    deduped
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.discount"), F.col("d.channel"), F.col("d.order_date"))
)

flagged = (
    joined
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "island_id",
        "order_date",
        "session_start_flag",
        "session_id",
        F.first("session_start_flag", ignorenulls=True).over(w4).alias("first_known_session_start_flag"),
        F.sum("session_id").over(w5).alias("session_id_sum_90d"),
        F.row_number().over(w6).alias("seq_no"),
    )
)
