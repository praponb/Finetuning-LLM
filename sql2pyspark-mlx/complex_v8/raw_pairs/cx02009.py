from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_event_at_year_ticket_id_by_e = Window.partitionBy(F.year("event_at"), "ticket_id").orderBy(F.col("event_at").desc())
w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_to_date_ticket_id_b = Window.partitionBy(F.to_date("event_at"), "ticket_id").orderBy("event_at")
w_event_at_to_date_ticket_id_b_2 = Window.partitionBy(F.to_date("event_at"), "ticket_id").orderBy("event_at", "id")
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_event_at_year_ticket_id_by_e_2 = Window.partitionBy(F.year("event_at"), "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_event_at_tercile_last_event = Window.partitionBy("event_at_tercile", F.month("last_event_at"))
w_all_by_last_event_at_running = Window.orderBy("last_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_last_event_at_prior_2 = Window.orderBy("last_event_at", "id").rowsBetween(-2, -1)
w_all_by_event_at_tercile = Window.orderBy("event_at_tercile", "id")

base = (
    df_ticket_events
    .filter((F.col("event_type") == "assigned") & (F.col("ticket_id") <= 38))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

joined_ticket = (
    base.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.actor_clean",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

metrics = (
    joined_ticket
    .select(
        "id",
        "ticket_id",
        "actor",
        "actor_clean",
        "ticket_tags",
        "ticket_priority",
        F.round(F.cume_dist().over(w_ticket_id_by_event_at), 3).alias("event_at_cume_dist"),
        F.round(F.cume_dist().over(w_event_at_year_ticket_id_by_e), 3).alias("event_at_cume_dist_2"),
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
        F.last("event_at").over(w_ticket_id_by_event_at_full).alias("last_event_at"),
        F.round(F.cume_dist().over(w_event_at_to_date_ticket_id_b), 3).alias("event_at_cume_dist_3"),
        F.lead("event_at", 3).over(w_event_at_to_date_ticket_id_b_2).alias("next3_event_at"),
        F.ntile(3).over(w_actor_by_event_at).alias("event_at_tercile"),
        F.rank().over(w_event_at_year_ticket_id_by_e).alias("event_at_rank"),
        F.lag("event_at", 2).over(w_event_at_year_ticket_id_by_e_2).alias("prev2_event_at"),
    )
)

first_rows = (
    metrics
    .filter(F.col("event_at_rank") == 1)
    .drop("event_at_rank")
)

windowed = (
    first_rows
    .select(
        "*",
        F.size(F.collect_set("actor").over(w_event_at_tercile_last_event)).alias("distinct_actor_in_event_at_tercile_last_event_at"),
        F.first("next3_event_at", ignorenulls=True).over(w_all_by_last_event_at_running).alias("first_known_next3_event_at"),
        F.max("event_at_cume_dist_3").over(w_all_by_last_event_at_prior_2).alias("event_at_cume_dist_3_max_2_prior"),
        F.row_number().over(w_all_by_event_at_tercile).alias("event_at_tercile_seq"),
        F.round(F.percent_rank().over(w_all_by_event_at_tercile), 3).alias("event_at_tercile_percent_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "ticket_id",
        "actor",
        "actor_clean",
        "ticket_priority",
        "event_at_cume_dist",
        "event_at_cume_dist_2",
        "first_known_event_at",
        "last_event_at",
        "event_at_cume_dist_3",
        "event_at_tercile",
        "distinct_actor_in_event_at_tercile_last_event_at",
        "first_known_next3_event_at",
        "event_at_cume_dist_3_max_2_prior",
        "event_at_tercile_seq",
        F.lower(F.regexp_replace(F.trim(F.col("ticket_priority")), " ", "_")).alias("ticket_priority_slug"),
        F.least(F.col("event_at_cume_dist_3"), F.col("event_at_cume_dist")).alias("least_event_at_cume_dist_3_event_at_cume_dist"),
    )
)
