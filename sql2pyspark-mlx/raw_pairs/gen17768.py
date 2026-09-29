from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.orderBy("paid_at", "id")
w3 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w5 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("amount_scaled", "id")
w7 = Window.partitionBy("method")
w8 = Window.partitionBy("method").orderBy(F.col("is_new_session").desc(), "id")

filtered = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-10-02 20:00:00").cast("timestamp"))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("amount_scaled", F.round((F.col("amount") * 1.07), 2))
)

enriched = (
    filtered
    .select(
        "*",
        F.sum("amount_scaled").over(w).alias("amount_scaled_sum_30d"),
        F.lead("amount_filled", 1, 0).over(w2).alias("next_amount_filled"),
        F.count("amount_filled").over(w3).alias("amount_filled_count_90d"),
    )
)

flagged = (
    enriched
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w4).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w4) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_session").over(w5))

enriched_payments = (
    islands
    .select(
        "id",
        "order_id",
        "method",
        "amount_filled",
        "is_new_session",
        F.lead("amount_filled", 1).over(w6).alias("next_amount_filled_2"),
        F.stddev_samp("burst_id").over(w7).alias("method_stddev_samp_burst_id"),
        F.row_number().over(w8).alias("event_seq"),
    )
)

result = (
    enriched_payments
    .groupBy("method_stddev_samp_burst_id")
    .agg(
        F.max(F.when(F.col("method") == "card", F.col("event_seq"))).alias("card_max_event_seq"),
        F.round(F.avg("amount_filled"), 2).alias("avg_amount_filled"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
    .orderBy(F.col("avg_amount_filled").asc_nulls_first())
)
