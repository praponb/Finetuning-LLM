from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_first_event_at_to_date = Window.partitionBy(F.to_date("first_event_at"))
w_row_count_by_total_event_typ = Window.partitionBy("row_count").orderBy("total_event_type_len", "event_type_len")

cleaned = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-24 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

latest = (
    cleaned
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

grouped = (
    latest
    .groupBy("event_type_len")
    .agg(
        F.count("*").alias("row_count"),
        F.min("event_at").alias("first_event_at"),
        F.sum("event_type_len").alias("total_event_type_len"),
        F.round(F.stddev("event_type_len"), 2).alias("stddev_event_type_len"),
    )
    .filter(F.col("row_count") >= 2)
)

sequenced = (
    grouped
    .select(
        "event_type_len",
        "first_event_at",
        "stddev_event_type_len",
        F.min("event_type_len").over(w_first_event_at_to_date).alias("first_event_at_to_date_min_event_type_len"),
        (F.col("row_count") - F.lag("row_count").over(w_row_count_by_total_event_typ)).alias("row_count_change"),
        F.dense_rank().over(w_row_count_by_total_event_typ).alias("total_event_type_len_dense_rank"),
        F.lead("stddev_event_type_len", 1, 0).over(w_row_count_by_total_event_typ).alias("next_stddev_event_type_len"),
        F.ntile(3).over(w_row_count_by_total_event_typ).alias("total_event_type_len_tercile"),
        F.round(F.percent_rank().over(w_row_count_by_total_event_typ), 3).alias("total_event_type_len_percent_rank"),
    )
)

derived = (
    sequenced
    .filter(F.col("total_event_type_len_dense_rank") <= 7)
    .select(
        "event_type_len",
        "stddev_event_type_len",
        "first_event_at_to_date_min_event_type_len",
        "row_count_change",
        "next_stddev_event_type_len",
        (F.floor(F.col("row_count_change") / 5) * 5).alias("row_count_change_bucket"),
        F.abs(F.col("event_type_len") - 6).alias("event_type_len_distance"),
    )
)

result = (
    derived
    .select(
        "event_type_len",
        "stddev_event_type_len",
        "first_event_at_to_date_min_event_type_len",
        "row_count_change_bucket",
        "event_type_len_distance",
    )
    .orderBy(F.col("row_count_change_bucket").desc(), "stddev_event_type_len")
)
