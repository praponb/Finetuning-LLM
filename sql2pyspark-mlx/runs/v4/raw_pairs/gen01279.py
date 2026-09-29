from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date_plus_months", "id")
w2 = Window.partitionBy("status").orderBy("delivered_date_plus_months", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("carrier", "status")
w5 = Window.partitionBy("status", "carrier").orderBy("carrier_stddev_samp_delivered_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("carrier").orderBy("event_seq", "id")
w_running = Window.partitionBy("status", "carrier").orderBy("prev_delivered_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("carrier").orderBy(F.col("last_known_event_seq").desc(), "id")
w_hist = Window.partitionBy("status").orderBy("last_known_event_seq", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.partitionBy("carrier", "status").orderBy(F.col("last_known_event_seq").desc_nulls_first())
w11 = Window.partitionBy("carrier").orderBy(F.col("event_seq").desc())

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "carrier",
        "delivered_date",
        "status",
        F.month("delivered_date").alias("delivered_date_month"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("carrier_clean"),
    )
)

valid = (
    filtered
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-10-19"))) & F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "delivered_date",
        "status",
        "delivered_date_month",
        "carrier_clean",
        F.add_months("delivered_date", 12).alias("delivered_date_plus_months"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.avg("delivered_date_month").over(w2).alias("delivered_date_month_avg_6"),
    )
)

enriched = (
    with_windows
    .withColumn("event_seq_share", F.round((F.col("event_seq") / F.sum("event_seq").over(w3)), 4))
    .withColumn("carrier_stddev_samp_delivered_date_month", F.stddev_samp("delivered_date_month").over(w4))
)

enriched_shipments = (
    enriched
    .select(
        "*",
        F.last("event_seq", ignorenulls=True).over(w5).alias("last_known_event_seq"),
        F.lag("delivered_date_month", 1).over(w6).alias("prev_delivered_date_month"),
    )
)

scored = (
    enriched_shipments
    .select(
        "*",
        F.sum("event_seq_share").over(w_running).alias("running_event_seq_share"),
        F.ntile(5).over(w8).alias("last_known_event_seq_bucket"),
        F.avg("delivered_date_month_avg_6").over(w_hist).alias("delivered_date_month_avg_6_avg_3"),
        F.cume_dist().over(w10).alias("last_known_event_seq_cume_dist"),
    )
)

deduped = (
    scored
    .withColumn("rn", F.rank().over(w11))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "carrier",
        "delivered_date",
        "status",
        "delivered_date_month",
        "carrier_clean",
        "delivered_date_plus_months",
        "carrier_filled",
        "event_seq",
    )
)
