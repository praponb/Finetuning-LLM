from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_quarter_runn = Window.orderBy(F.col("event_at_quarter").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_to_date = Window.partitionBy(F.to_date("event_at"))
w_all_by_running_event_at_quar = Window.orderBy(F.col("running_event_at_quarter").desc(), "id")
w_actor_by_running_event_at_qu = Window.partitionBy("actor").orderBy(F.col("running_event_at_quarter").desc(), "id")
w_event_type_actor = Window.partitionBy("event_type", "actor")

recent = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
    )
)

windowed = (
    recent
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "actor_event_type_label",
        "event_at_quarter",
        "event_at_label",
        F.sum("event_at_quarter").over(w_all_by_event_at_quarter_runn).alias("running_event_at_quarter"),
        F.sum("event_at_quarter").over(w_event_at_to_date).alias("event_at_to_date_sum_event_at_quarter"),
        F.round(F.col("event_at_quarter") / F.nullif(F.sum("event_at_quarter").over(w_event_at_to_date), F.lit(0)), 4).alias("event_at_quarter_share_of_event_at_to_date"),
    )
)

labeled = (
    windowed
    .filter(F.col("actor_event_type_label").isNotNull())
    .withColumn("event_at_quarter_distance", F.abs(F.col("event_at_quarter") - 2))
)

with_trends = (
    labeled
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "actor_event_type_label",
        "running_event_at_quarter",
        "event_at_to_date_sum_event_at_quarter",
        "event_at_quarter_share_of_event_at_to_date",
        "event_at_quarter_distance",
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_running_event_at_quar))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lead("event_at_quarter_distance", 3).over(w_actor_by_running_event_at_qu).alias("next3_event_at_quarter_distance"),
        F.round(F.col("event_at_quarter") / F.nullif(F.sum("event_at_quarter").over(w_event_type_actor), F.lit(0)), 4).alias("event_at_quarter_share_of_event_type_actor"),
    )
)

result = (
    with_trends
    .filter(F.col("next3_event_at_quarter_distance") <= 3)
    .select(
        "id",
        "event_at",
        "actor",
        "actor_event_type_label",
        "running_event_at_quarter",
        "event_at_quarter_distance",
        "next3_event_at_quarter_distance",
    )
)
