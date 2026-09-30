from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_start_year_ticket_id = Window.partitionBy(F.year("session_start"), "ticket_id").orderBy(F.col("n_events").desc(), "ticket_id", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id_n_events = Window.partitionBy("session_id", "n_events")
w_session_start_year_ticket_id_2 = Window.partitionBy(F.year("session_start"), "ticket_id").orderBy(F.col("n_events").desc(), "ticket_id", "session_id")

cleaned_events = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 28)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = cleaned_events.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_ticket_id_running))

session_summary = (
    sessionized
    .groupBy("ticket_id", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

windowed = (
    session_summary
    .select(
        "ticket_id",
        "session_id",
        "session_end",
        "duration_min",
        F.max("duration_min").over(w_session_start_year_ticket_id).alias("max_duration_min_so_far"),
        F.size(F.collect_set("ticket_id").over(w_session_id_n_events)).alias("distinct_ticket_id_in_session_id_n_events"),
        F.count(F.lit(1)).over(w_session_id_n_events).alias("rows_in_session_id_n_events"),
        F.ntile(10).over(w_session_start_year_ticket_id_2).alias("n_events_decile"),
    )
)

result = (
    windowed
    .groupingSets(
        [["distinct_ticket_id_in_session_id_n_events", "n_events_decile", "ticket_id"], ["distinct_ticket_id_in_session_id_n_events"], []],
        "distinct_ticket_id_in_session_id_n_events", "n_events_decile", "ticket_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
    )
    .orderBy(
        F.col("distinct_ticket_id_in_session_id_n_events").asc_nulls_last(),
        F.col("n_events_decile").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
