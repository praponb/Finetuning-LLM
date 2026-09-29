from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status").orderBy("shipped_date_period", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("status", "carrier").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy(F.col("shipped_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("shipped_date_period", "id")
w5 = Window.partitionBy("order_id", "status").orderBy(F.col("days_since_delivered_date_avg_2").desc(), "id")
w6 = Window.partitionBy("status").orderBy("shipped_date_period", "id")
w7 = Window.partitionBy("status").orderBy("shipped_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_id").orderBy("shipped_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("order_status")
w10 = Window.partitionBy("order_id")

filtered = (
    df_shipments
    .filter(F.col("shipped_date") > F.lit("2023-03-09").cast("date"))
    .withColumn("shipped_date_period", F.date_trunc("month", "shipped_date"))
    .withColumn("days_since_delivered_date", F.datediff(F.current_date(), "delivered_date"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("DIRECT")))
)

scored = (
    filtered
    .select(
        "*",
        F.avg("days_since_delivered_date").over(w).alias("days_since_delivered_date_avg_5_prior"),
        F.avg("days_since_delivered_date").over(w2).alias("days_since_delivered_date_avg_2"),
        F.count("days_since_delivered_date").over(w3).alias("days_since_delivered_date_count_30d"),
        F.lead("cost", 2, 0).over(w4).alias("next_cost"),
    )
)

deduped = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

joined = (
    deduped
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.status").alias("order_status"),
        F.col("b.shipping_fee"),
        F.col("b.discount"),
    )
)

flagged = (
    joined
    .withColumn("session_start_flag", F.when(F.col("shipped_date_period") - F.lag("shipped_date_period").over(w6) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_shipments = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w7))

prev_events = (
    sessions_shipments
    .withColumn("prev_shipped_date_period", F.lag("shipped_date_period").over(w4))
)

with_gaps = (
    prev_events
    .withColumn("session_start_flag_2", F.when(F.col("prev_shipped_date_period").isNull() | (F.col("shipped_date_period") > F.col("prev_shipped_date_period") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id_2", F.sum("session_start_flag_2").over(w8))

with_windows = (
    sessionized
    .select(
        "*",
        F.avg("days_since_delivered_date_avg_2").over(w9).alias("order_status_avg_days_since_delivered_date_avg_2"),
        F.round((F.col("cost") / F.sum("cost").over(w10)), 4).alias("cost_share"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "days_since_delivered_date_avg_5_prior",
        "days_since_delivered_date_avg_2",
        "days_since_delivered_date_count_30d",
        "next_cost",
        "order_status",
    )
    .orderBy(F.col("cost").asc_nulls_last())
)
