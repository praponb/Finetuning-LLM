from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("ticket_id").orderBy(F.col("event_type_len").desc(), F.col("id").desc())

cleaned = (
    df_ticket_events
    .filter(F.col("event_type").isin("replied", "created"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

latest_per_key = (
    cleaned
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

without_matches = (
    latest_per_key.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isNotNull()) & (F.col("t.event_type").isin("resolved", "replied", "escalated")), "left_anti")
)

result = (
    without_matches
    .rollup("event_type", "ticket_id")
    .agg(F.min("event_type_len").alias("min_event_type_len"))
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
