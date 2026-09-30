from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_at_year_by_days = Window.partitionBy("actor", F.year("event_at")).orderBy("days_since_event_at", "id")
w_actor_event_at_year_by_days_2 = Window.partitionBy("actor", F.year("event_at")).orderBy("days_since_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_or_unknown_event_type = Window.partitionBy("actor_or_unknown", "event_type")
w_all_by_days_since_event_at = Window.orderBy(F.col("days_since_event_at").desc(), "id")
w_latest = Window.partitionBy("actor").orderBy(F.col("distinct_ticket_id_in_actor_or_unknown_event_typ").desc(), F.col("id").desc())

prepared = (
    df_ticket_events
    .filter(F.col("event_type") != "reopened")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.concat_ws("-", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

metrics = (
    prepared
    .select(
        "*",
        F.lag("event_at", 2).over(w_actor_event_at_year_by_days).alias("prev2_event_at"),
        F.last("days_since_event_at").over(w_actor_event_at_year_by_days_2).alias("last_days_since_event_at"),
        F.size(F.collect_set("ticket_id").over(w_actor_or_unknown_event_type)).alias("distinct_ticket_id_in_actor_or_unknown_event_typ"),
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_actor_or_unknown_event_type), F.lit(0)), 4).alias("days_since_event_at_share_of_actor_or_unknown_ev"),
        F.min("days_since_event_at").over(w_actor_or_unknown_event_type).alias("actor_or_unknown_event_type_min_days_since_event"),
        F.rank().over(w_all_by_days_since_event_at).alias("days_since_event_at_rank"),
    )
)

unique_rows = (
    metrics
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    unique_rows
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor_event_type_label",
        "prev2_event_at",
        "last_days_since_event_at",
        "distinct_ticket_id_in_actor_or_unknown_event_typ",
        "days_since_event_at_share_of_actor_or_unknown_ev",
        "actor_or_unknown_event_type_min_days_since_event",
        F.round((F.unix_timestamp(F.col("prev2_event_at")) - F.unix_timestamp(F.col("event_at"))) / 3600, 1).alias("hours_event_at_to_prev2_event_at"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("prev2_event_at")).alias("days_since_prev2_event_at"),
        F.abs(F.col("days_since_event_at") - 245).alias("days_since_event_at_distance"),
    )
)

result = (
    scored
    .select(
        "id",
        F.col("hours_event_at_to_prev2_event_at").alias("m_hours_event_at_to_prev2_event_at"),
        F.col("days_since_event_at_share_of_actor_or_unknown_ev").alias("m_days_since_event_at_share_of_actor_or_unknown_"),
    )
    .unpivot(
        ids=["id"],
        values=["m_hours_event_at_to_prev2_event_at", "m_days_since_event_at_share_of_actor_or_unknown_"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .orderBy("id", "measure")
)
