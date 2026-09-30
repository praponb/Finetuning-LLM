from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_group_actor_by_event_a = Window.partitionBy("actor_group", "actor").orderBy("event_at", "id")
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")
w_event_at_month_by_event_at_r = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_month_by_event_at_t = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc())
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy("event_at")

base_events = (
    df_ticket_events
    .filter(~F.col("actor").isin("system", "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("agent", "system"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
)

ranked = (
    base_events
    .select(
        "*",
        F.rank().over(w_actor_group_actor_by_event_a).alias("event_at_rank"),
        F.round(F.cume_dist().over(w_actor_group_actor_by_event_a), 3).alias("event_at_cume_dist"),
        F.lag("event_at").over(w_actor_ticket_id_by_event_at).alias("prev_event_at"),
        F.first("event_at", ignorenulls=True).over(w_event_at_month_by_event_at_r).alias("first_known_event_at"),
        F.ntile(3).over(w_actor_ticket_id_by_event_at).alias("event_at_tercile"),
        F.dense_rank().over(w_event_at_month_by_event_at_t).alias("event_at_dense_rank"),
        F.rank().over(w_actor_ticket_id_by_event_at_2).alias("event_at_rank_3"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.ntile(4).over(w_actor_group_actor_by_event_a).alias("event_at_quartile"),
    )
)

enriched = (
    ranked
    .filter(F.col("event_at_quartile").between(4, 7))
    .select(
        "*",
        F.to_date(F.col("prev_event_at")).alias("prev_event_at_date"),
        F.round(F.col("event_at_tercile") / F.nullif(F.col("event_at_cume_dist"), F.lit(0)), 3).alias("event_at_tercile_per_event_at_cume_dist"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

result = (
    enriched
    .select(
        "id",
        "actor",
        "actor_group",
        "event_type_prefix",
        "event_at_rank",
        "event_at_cume_dist",
        "prev_event_at",
        "first_known_event_at",
        "event_at_tercile",
        "event_at_dense_rank",
        "minutes_since_prev_event_at",
        "event_at_quartile",
        "prev_event_at_date",
        "event_at_tercile_per_event_at_cume_dist",
        F.coalesce(F.col("event_at_dense_rank"), F.lit(0)).alias("event_at_dense_rank_filled"),
    )
    .orderBy(F.col("id").desc())
)
