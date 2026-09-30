from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_or_unknown_by_tic = Window.partitionBy("event_type_or_unknown").orderBy("ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_or_unknown_by_tic_2 = Window.partitionBy("event_type_or_unknown").orderBy(F.col("ticket_csat").desc(), "id")
w_event_type_or_unknown_by_tic_3 = Window.partitionBy("event_type_or_unknown").orderBy(F.col("ticket_csat").desc(), "id").rowsBetween(-1, -1)
w_event_type_or_unknown_by_tic_4 = Window.partitionBy("event_type_or_unknown").orderBy("ticket_csat")

cleaned = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "event_type",
        "ticket_csat",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

labeled = (
    scored
    .filter(F.col("ticket_csat") >= 2)
    .withColumn("event_type_or_unknown_prefix", F.substring(F.trim(F.col("event_type_or_unknown")), 1, 2))
)

with_trends = (
    labeled
    .select(
        "*",
        F.first("ticket_csat", ignorenulls=True).over(w_event_type_or_unknown_by_tic).alias("first_known_ticket_csat"),
        F.row_number().over(w_event_type_or_unknown_by_tic_2).alias("ticket_csat_seq"),
        F.round(F.avg("ticket_csat").over(w_event_type_or_unknown_by_tic_3), 2).alias("ticket_csat_avg_1_prior"),
        F.round(F.cume_dist().over(w_event_type_or_unknown_by_tic_4), 3).alias("ticket_csat_cume_dist"),
        (F.col("ticket_csat") - F.lag("ticket_csat").over(w_event_type_or_unknown_by_tic_2)).alias("ticket_csat_change"),
    )
)

screened = (
    with_trends
    .crossJoin(with_trends.agg(F.min("ticket_csat_cume_dist").alias("overall_min_ticket_csat_cume_dist")))
    .filter(F.col("ticket_csat_cume_dist") > F.col("overall_min_ticket_csat_cume_dist"))
    .drop("overall_min_ticket_csat_cume_dist")
)

result = (
    screened
    .select(
        "id",
        "event_type",
        "ticket_csat",
        "event_type_or_unknown_prefix",
        "first_known_ticket_csat",
        "ticket_csat_seq",
        "ticket_csat_avg_1_prior",
        "ticket_csat_change",
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
    .orderBy(F.col("first_known_ticket_csat").desc_nulls_first(), "id")
    .limit(10)
)
