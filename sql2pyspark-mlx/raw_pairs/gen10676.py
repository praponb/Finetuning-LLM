from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("status") == "FAILED", 1).otherwise(0).alias("is_failed"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

filtered = (
    cleaned
    .filter((F.col("paid_at") >= F.lit("2023-09-24 14:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "*",
        F.trim("method").alias("clean_method"),
        (F.col("is_failed") * F.col("amount")).alias("is_failed_times_amount"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

flagged = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

matched_keys = (
    islands
    .select("id")
    .intersectAll(
        df_payments
        .filter(F.col("paid_at") >= "2024-05-30")
        .select("id")
    )
)

reconciled = (
    islands
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "*",
        F.count("amount").over(w3).alias("amount_count_7d"),
        F.sum("amount").over(w4).alias("amount_sum_7d"),
    )
)

cleaned_payments = (
    enriched
    .filter((F.col("paid_at") >= F.lit("2023-09-22").cast("date")) & F.col("method").isNotNull())
    .select("*", F.when(F.col("method") == "card", 1).otherwise(0).alias("is_card"))
)

result = (
    cleaned_payments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["order_status", "channel"], ["order_status"], []],
        F.col("p.status").alias("order_status"),
        F.col("p.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.island_id")).alias("min_island_id"),
        F.grouping_id().alias("gid"),
    )
)
