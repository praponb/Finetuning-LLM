from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id")
w_ticket_priority_by_event_at = Window.partitionBy("ticket_priority").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = df_ticket_events.filter((F.col("event_type") == "created") & (F.col("actor") == "system"))

latest = (
    base
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

qualified = (
    latest.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

qualified_2 = (
    qualified.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

combined_details_ticket = (
    qualified_2.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.priority").alias("ticket_priority"),
    )
)

with_trends = (
    combined_details_ticket
    .select(
        "*",
        F.row_number().over(w_event_type_actor_by_event_at).alias("rn"),
        F.nth_value("event_at", 2).over(w_ticket_priority_by_event_at).alias("second_event_at"),
        F.first("event_at").over(w_ticket_priority_by_event_at).alias("first_event_at"),
        F.first("event_at", ignorenulls=True).over(w_event_type_actor_by_event_at_2).alias("first_known_event_at"),
    )
)

rollup_base = (
    with_trends
    .groupBy("actor")
    .agg(
        F.round(F.stddev("rn"), 2).alias("stddev_rn"),
        F.max("rn").alias("max_rn"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("actor").isin("customer", "agent"), F.col("rn"))).alias("rn_actor_in_customer_agent"),
    )
)

result = (
    rollup_base
    .select("actor", F.col("cnt").alias("m_cnt"), F.col("max_rn").alias("m_max_rn"))
    .unpivot(
        ids=["actor"],
        values=["m_cnt", "m_max_rn"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("actor", "metric_name")
)
