from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_event_at_month_actor_by_even = Window.partitionBy(F.month("event_at"), "actor").orderBy("event_at")
w_event_at_month_actor_by_even_2 = Window.partitionBy(F.month("event_at"), "actor").orderBy("event_at", "id")

cleaned_events = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 30) & (~F.col("actor").isin("system", "agent")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("|", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

annotated = (
    cleaned_events
    .filter(F.col("ticket_id") >= 38)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_event_type_label",
        "event_type_ref",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
    )
)

latest = (
    annotated
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

windowed = (
    latest
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "actor_event_type_label",
        "event_type_ref",
        "event_at_label",
        F.rank().over(w_event_at_month_actor_by_even).alias("event_at_rank"),
        F.lead("event_at", 3).over(w_event_at_month_actor_by_even_2).alias("next3_event_at"),
    )
)

with_activity = (
    windowed
    .crossJoin(windowed.agg(F.max("event_at_rank").alias("overall_max_event_at_rank")))
    .filter(F.col("event_at_rank") < F.col("overall_max_event_at_rank"))
    .drop("overall_max_event_at_rank")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(~F.col("event_type").isin("reopened", "escalated"), 1)).alias("event_event_type_count"),
        F.count(F.when(F.col("event_type").isin("reopened", "created"), 1)).alias("event_event_type_count_2"),
    )
)

with_counts = (
    with_activity.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        F.coalesce(F.col("t.event_event_type_count_2"), F.lit(0)).alias("event_event_type_count_2"),
    )
)

scored = (
    with_counts
    .filter(F.col("event_at_label").isNotNull())
    .select(
        "*",
        (F.col("event_at_rank") - F.col("event_event_type_count")).alias("event_at_rank_minus_event_event_type_count"),
        F.date_format(F.col("next3_event_at"), "yyyy-MM").alias("next3_event_at_label"),
        F.substring(F.trim(F.col("actor_event_type_label")), 1, 4).alias("actor_event_type_label_prefix"),
    )
)

result = (
    scored
    .filter(F.to_date(F.col("next3_event_at")) < F.lit("2025-03-14").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor_event_type_label",
        "event_type_ref",
        "event_at_label",
        "event_event_type_count",
        "next3_event_at_label",
        "actor_event_type_label_prefix",
        F.concat_ws(": ", F.col("next3_event_at_label"), F.col("actor_event_type_label")).alias("next3_event_at_label_actor_event_type_label_labe"),
        F.abs(F.col("event_event_type_count_2") - 4).alias("event_event_type_count_2_distance"),
    )
    .orderBy("event_at", F.col("actor_event_type_label").asc_nulls_last(), "id")
    .limit(10)
)
