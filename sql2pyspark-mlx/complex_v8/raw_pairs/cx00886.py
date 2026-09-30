from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id")

base_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_at") < F.lit("2025-02-24 11:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

without_matches = (
    base_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.csat") < 5) & (F.size(F.col("t.tags")) >= 2), "left_semi")
)

deduped = (
    without_matches
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .rollup("ticket_id", "event_type", "actor")
    .agg(F.array_sort(F.collect_set("event_type")).alias("event_type_list"))
    .select(
        "ticket_id",
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "event_type_list",
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
    )
)
