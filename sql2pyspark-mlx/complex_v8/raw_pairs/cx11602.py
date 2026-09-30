from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("actor", "event_type").orderBy("event_at", "id")
w_latest_2 = Window.partitionBy("actor").orderBy(F.col("days_since_event_at").desc(), F.col("id").desc())

scoped_events = (
    df_ticket_events
    .filter((F.col("actor").isin("agent", "system")) & (F.month(F.col("event_at")).isin(1, 3)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
    )
)

latest = (
    scoped_events
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped = (
    latest
    .withColumn("recency_rank", F.row_number().over(w_latest_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .groupingSets(
        [["ticket_id", "event_type", "actor"], ["ticket_id"], []],
        "ticket_id", "event_type", "actor",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
    )
    .select(
        "ticket_id",
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "grouping_level",
        "event_type_list",
    )
)
