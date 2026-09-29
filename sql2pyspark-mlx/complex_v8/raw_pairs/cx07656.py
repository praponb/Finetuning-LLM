from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_all_by_hours_since_open_ties = Window.orderBy("hours_since_open")

in_window = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

windowed = (
    in_window
    .withColumn("hours_since_open_share_of_priority", F.round(F.col("hours_since_open") / F.nullif(F.sum("hours_since_open").over(w_priority), F.lit(0)), 4))
    .withColumn("hours_since_open_cume_dist", F.round(F.cume_dist().over(w_all_by_hours_since_open_ties), 3))
)

result = (
    windowed
    .groupBy("ticket_id")
    .pivot("priority", ["P1", "P2", "P4"])
    .agg(F.avg("hours_since_open_cume_dist").alias("avg_v"), F.sum("hours_since_open_cume_dist").alias("sum_v"))
)
