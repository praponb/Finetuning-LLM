from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_to_date_by_event_at = Window.partitionBy(F.to_date("event_at")).orderBy("event_at", "id")

valid = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")) & (F.col("actor") != "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.round(F.cume_dist().over(w_event_at_to_date_by_event_at), 3).alias("event_at_cume_dist"),
        F.row_number().over(w_event_at_to_date_by_event_at).alias("seq_no"),
        F.lag("event_at").over(w_event_at_to_date_by_event_at).alias("prev_event_at"),
        F.lag("event_at_year", 2).over(w_event_at_to_date_by_event_at).alias("prev2_event_at_year"),
    )
)

top_per_group = windowed.filter(F.col("seq_no") == 1)

result = (
    top_per_group
    .groupBy("actor", "event_at")
    .pivot("event_type", ["replied", "resolved"])
    .agg(F.max("event_at_cume_dist").alias("max_v"), F.count("event_at_cume_dist").alias("n"))
    .orderBy("actor", "event_at")
)
