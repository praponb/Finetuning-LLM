from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy(F.year("paid_at"), "order_id")
w5 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-06-02").cast("date")) & F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"),
        F.trim("method").alias("clean_method"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

with_gaps = (
    base
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = (
    with_gaps
    .select(
        "id",
        "order_id",
        "paid_at",
        "paid_at_label",
        "clean_method",
        "method_filled",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

flagged = (
    sessions_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "order_id",
        "paid_at",
        "method_filled",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

matched_keys = (
    sessionized
    .select("order_id")
    .subtract(
        df_payments
        .filter(F.col("amount") > 1250)
        .select("order_id")
    )
)

reconciled = (
    sessionized
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter((F.col("paid_at") > F.lit("2023-09-12").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "*",
        F.round("island_id", 0).alias("island_id_rounded"),
        F.round((F.col("island_id") * 1.1), 2).alias("island_id_adjusted"),
    )
)

metrics = (
    filtered
    .filter(F.col("order_id").isNull())
    .select(
        "method_filled",
        "island_id_adjusted",
        F.count("island_id_rounded").over(w3).alias("island_id_rounded_count_7d"),
        F.avg("island_id").over(w4).alias("paid_at_year_avg_island_id"),
        F.count("island_id").over(w5).alias("island_id_count_90d"),
    )
)

result = (
    metrics
    .select("island_id_adjusted", "island_id_count_90d")
    .orderBy("island_id_adjusted")
)
