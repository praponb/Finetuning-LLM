from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.substring("status", 1, 3).alias("status_prefix"),
    )
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_payments
    .groupBy(
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
        F.when(F.col("island_id") >= 9, "PREMIUM").otherwise("STANDARD").alias("island_id_tier"),
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
    )
    .agg(
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max(F.when(F.col("status") == "FAILED", F.col("island_id"))).alias("failed_max_island_id"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min_2"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
    .filter(F.col("duration_min") >= 0)
)
