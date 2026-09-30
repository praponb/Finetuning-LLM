from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_ticket_id_event_at_year_by_e = Window.partitionBy("ticket_id", F.year("event_at")).orderBy("event_at_quintile_minus_avg", "id")
w_all_by_next_avg_event_at_qui = Window.orderBy("next_avg_event_at_quintile")
w_next3_event_at_quintile_even = Window.partitionBy("next3_event_at_quintile", "event_type")
w_event_at_year_event_at_quint = Window.partitionBy(F.year("event_at"), "event_at_quintile").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_next3_event_at_year_by_next3 = Window.partitionBy(F.year("next3_event_at")).orderBy(F.col("next3_event_at").desc())
w_event_at_year_event_at_quint_2 = Window.partitionBy(F.year("event_at"), "event_at_quintile").orderBy(F.col("event_at").desc())
w_next3_event_at_year_by_next3_2 = Window.partitionBy(F.year("next3_event_at")).orderBy(F.col("next3_event_at").desc(), "id")

prepared = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(7, 13)) & (F.col("event_type") != "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label_2"),
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_ref",
        "event_at_label",
        "event_at_label_2",
        F.ntile(5).over(w_actor_by_event_at).alias("event_at_quintile"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.lead("event_at", 3).over(w_all_by_event_at).alias("next3_event_at"),
    )
)

event_type_stats = (
    with_windows
    .groupBy("event_type")
    .agg(F.avg("event_at_quintile").alias("avg_event_at_quintile"))
)

above_average = (
    with_windows.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.event_at_quintile") > F.col("g.avg_event_at_quintile"))
    .select(
        "s.*",
        "g.avg_event_at_quintile",
        F.round(F.col("s.event_at_quintile") - F.col("g.avg_event_at_quintile"), 2).alias("event_at_quintile_minus_avg"),
    )
)

with_windows_2 = (
    above_average
    .select(
        "*",
        F.lead("avg_event_at_quintile").over(w_ticket_id_event_at_year_by_e).alias("next_avg_event_at_quintile"),
        F.lead("event_at_quintile", 3).over(w_ticket_id_event_at_year_by_e).alias("next3_event_at_quintile"),
    )
)

with_windows_3 = (
    with_windows_2
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor_ref",
        "event_at_label",
        "event_at_label_2",
        "event_at_quintile",
        "next3_event_at",
        "next_avg_event_at_quintile",
        "next3_event_at_quintile",
        F.round(F.cume_dist().over(w_all_by_next_avg_event_at_qui), 3).alias("next_avg_event_at_quintile_cume_dist"),
        F.count(F.lit(1)).over(w_next3_event_at_quintile_even).alias("rows_in_next3_event_at_quintile_event_type"),
        F.first("next_avg_event_at_quintile", ignorenulls=True).over(w_event_at_year_event_at_quint).alias("first_known_next_avg_event_at_quintile"),
        F.round(F.percent_rank().over(w_next3_event_at_year_by_next3), 3).alias("next3_event_at_percent_rank"),
        F.round(F.cume_dist().over(w_event_at_year_event_at_quint_2), 3).alias("event_at_cume_dist"),
        F.row_number().over(w_next3_event_at_year_by_next3_2).alias("rn"),
    )
)

derived = (
    with_windows_3
    .filter(F.col("first_known_next_avg_event_at_quintile") > 3.7)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_label_2",
        "next_avg_event_at_quintile",
        "next3_event_at_quintile",
        "next_avg_event_at_quintile_cume_dist",
        "rows_in_next3_event_at_quintile_event_type",
        "first_known_next_avg_event_at_quintile",
        "next3_event_at_percent_rank",
        "event_at_cume_dist",
        "rn",
        F.coalesce(F.col("next3_event_at_quintile"), F.lit(0)).alias("next3_event_at_quintile_filled"),
        F.year(F.col("next3_event_at")).alias("next3_event_at_year"),
    )
)

labeled = (
    derived
    .filter(F.hour(F.col("event_at")).between(6, 11))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_label_2",
        "next_avg_event_at_quintile",
        "next3_event_at_quintile",
        "rows_in_next3_event_at_quintile_event_type",
        "first_known_next_avg_event_at_quintile",
        "event_at_cume_dist",
        "rn",
        "next3_event_at_year",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

result = (
    labeled
    .select(
        "id",
        "event_at",
        "event_at_label_2",
        "next_avg_event_at_quintile",
        "rows_in_next3_event_at_quintile_event_type",
        "first_known_next_avg_event_at_quintile",
        "event_at_cume_dist",
        "rn",
        "event_at_label",
        F.substring(F.trim(F.col("event_at_label")), 1, 3).alias("event_at_label_prefix"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)
