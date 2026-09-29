from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_first_event_at_runn = Window.partitionBy("actor").orderBy(F.col("first_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_actor = Window.partitionBy("event_type", "actor")
w_all_by_next3_event_at_prior = Window.orderBy("next3_event_at", "id").rowsBetween(-3, -1)

filtered = (
    df_ticket_events
    .filter((F.to_date(F.col("event_at")) < F.lit("2025-02-24").cast("date")) & (F.col("event_type").isin("replied", "assigned")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "event_type",
        "actor",
        F.ntile(10).over(w_all_by_event_at).alias("event_at_decile"),
        F.lead("event_at", 3).over(w_all_by_event_at).alias("next3_event_at"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.ntile(3).over(w_all_by_event_at).alias("event_at_tercile"),
    )
)

with_trends = (
    with_windows
    .select(
        "id",
        "event_type",
        "actor",
        "next3_event_at",
        "event_at_tercile",
        F.last("first_event_at", ignorenulls=True).over(w_actor_by_first_event_at_runn).alias("last_known_first_event_at"),
        F.count(F.lit(1)).over(w_event_type_actor).alias("rows_in_event_type_actor"),
        F.round(F.avg("event_at_tercile").over(w_all_by_next3_event_at_prior), 2).alias("event_at_tercile_avg_3_prior"),
        F.first("event_at_decile", ignorenulls=True).over(w_actor_by_first_event_at_runn).alias("first_known_event_at_decile"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["actor", "event_type", "event_at_tercile"], ["actor"]],
        "actor", "event_type", "event_at_tercile",
    )
    .agg(
        F.sum("first_known_event_at_decile").alias("total_first_known_event_at_decile"),
        F.round(F.sum("rows_in_event_type_actor") / F.nullif(F.sum("event_at_tercile_avg_3_prior"), F.lit(0)), 3).alias("rows_in_event_type_actor_to_event_at_tercile_avg"),
    )
    .select(
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "event_at_tercile",
        "total_first_known_event_at_decile",
        "rows_in_event_type_actor_to_event_at_tercile_avg",
    )
)
