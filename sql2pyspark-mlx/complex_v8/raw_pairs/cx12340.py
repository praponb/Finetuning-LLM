from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_or_unknown = Window.partitionBy("event_type_or_unknown")
w_all_by_last_event_at_running = Window.orderBy("last_event_at", "event_type_or_unknown", "ticket_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

stats = (
    recent
    .filter(F.col("event_type_or_unknown") != "created")
    .groupBy("event_type_or_unknown", "ticket_id")
    .agg(
        F.round(100 * F.count(F.when(F.col("actor").isin("system", "customer"), 1)) / F.count("*"), 1).alias("pct_actor_in_system_customer"),
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

with_windows = (
    stats
    .withColumn("rows_in_event_type_or_unknown", F.count(F.lit(1)).over(w_event_type_or_unknown))
    .withColumn("first_known_first_event_at", F.first("first_event_at", ignorenulls=True).over(w_all_by_last_event_at_running))
)

result = (
    with_windows
    .select(
        "event_type_or_unknown",
        "ticket_id",
        "pct_actor_in_system_customer",
        "last_event_at",
        "rows_in_event_type_or_unknown",
        "first_known_first_event_at",
    )
)
