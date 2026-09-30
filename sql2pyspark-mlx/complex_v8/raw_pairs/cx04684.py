from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("event_type_or_unknown").orderBy(F.col("first_event_at").desc(), F.col("event_type").desc())

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

stats = (
    in_window
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("event_type").isin("reopened", "replied"), F.col("hours_since_open"))).alias("hours_since_open_event_type_in_reopened_repli"),
        F.percentile_approx("hours_since_open", 0.5).alias("median_hours_since_open"),
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("hours_since_open"), 2).alias("stddev_hours_since_open"),
    )
)

scored = (
    stats
    .select(
        "event_type",
        "hours_since_open_event_type_in_reopened_repli",
        "median_hours_since_open",
        "first_event_at",
        "cnt",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .select(
        "event_type",
        F.col("median_hours_since_open").alias("m_median_hours_since_open"),
        F.col("hours_since_open_event_type_in_reopened_repli").alias("m_hours_since_open_event_type_in_reopened_repli"),
    )
    .unpivot(
        ids=["event_type"],
        values=["m_median_hours_since_open", "m_hours_since_open_event_type_in_reopened_repli"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
