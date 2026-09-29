from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_by_event_at_f = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_by_event_at = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").desc(), "id")
w_event_at_month_by_event_at_t = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at").desc())

scoped_events = (
    df_ticket_events
    .filter((~F.col("actor").isin("agent", "customer")) & (F.col("event_type") != "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

ranked = (
    scoped_events
    .select(
        "*",
        F.nth_value("event_at", 2).over(w_event_at_month_by_event_at_f).alias("second_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_month_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.row_number().over(w_event_at_month_by_event_at).alias("rn"),
        F.round(F.percent_rank().over(w_event_at_month_by_event_at_t), 3).alias("event_at_percent_rank"),
        F.rank().over(w_event_at_month_by_event_at_t).alias("event_at_rank"),
        F.ntile(3).over(w_event_at_month_by_event_at).alias("event_at_tercile"),
    )
)

result = (
    ranked
    .select(
        "id",
        F.col("event_at_percent_rank").alias("event_at_percent_rank_v"),
        F.col("minutes_since_prev_event_at").alias("m_minutes_since_prev_event_at"),
    )
    .unpivot(
        ids=["id"],
        values=["event_at_percent_rank_v", "m_minutes_since_prev_event_at"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
