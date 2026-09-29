from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_year_event_type_by = Window.partitionBy(F.year("event_at"), "event_type").orderBy("event_at_date", "id")

valid = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

tagged = (
    valid
    .select(
        "*",
        F.concat(F.col("event_type_slug"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_slug_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug_2"),
        F.when(F.col("actor").isin("agent", "customer"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
)

metrics = (
    tagged
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_date",
        "actor_group",
        F.ntile(10).over(w_event_at_year_event_type_by).alias("event_at_date_decile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_year_event_type_by))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lag("event_at_date").over(w_event_at_year_event_type_by).alias("prev_event_at_date"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_date",
        "actor_group",
        "event_at_date_decile",
        "minutes_since_prev_event_at",
        "prev_event_at_date",
        F.coalesce(F.col("event_at_date_decile"), F.lit(0)).alias("event_at_date_decile_filled"),
    )
    .orderBy(F.col("event_at").asc(), "id")
    .limit(5)
)
