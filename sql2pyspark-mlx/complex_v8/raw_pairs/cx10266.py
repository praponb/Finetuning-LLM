from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_id_by_event_eve = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_event_at").desc_nulls_first(), "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_month = Window.orderBy(F.col("event_at_month").desc(), "id", "event_id")
w_event_type_event_actor_by_ne = Window.partitionBy("event_type", "event_actor").orderBy("next_event_event_at", "id", "event_id")
w_actor_ticket_id_by_last_even = Window.partitionBy("actor", "ticket_id").orderBy(F.col("last_event_event_at").desc(), "id", "event_id")
w_last_event_event_at_dense_ra = Window.partitionBy("last_event_event_at_dense_rank").orderBy("total_last_event_event_at_dense_rank", "last_event_event_at_dense_rank", "event_actor_group")
w_last_event_event_at_dense_ra_2 = Window.partitionBy("last_event_event_at_dense_rank").orderBy("total_last_event_event_at_dense_rank", "last_event_event_at_dense_rank", "event_actor_group").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_event_actor_groups = Window.partitionBy("distinct_event_actor_groups")
w_event_actor_group_last_event = Window.partitionBy("event_actor_group", "last_event_event_at_dense_rank").orderBy("distinct_event_actor_groups", "last_event_event_at_dense_rank", "event_actor_group")
w_last_event_event_at_dense_ra_3 = Window.partitionBy("last_event_event_at_dense_rank").orderBy("total_last_event_event_at_dense_rank")

cleaned_events = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(1, 2)) & (F.col("event_type").isin("reopened", "assigned", "created")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

exploded_detail = (
    cleaned_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (~F.col("t.actor").isin("agent", "customer")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_month",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

windowed = (
    exploded_detail
    .select(
        "*",
        F.last("event_event_at").over(w_actor_ticket_id_by_event_eve).alias("last_event_event_at"),
        F.lead("event_event_at").over(w_all_by_event_at_month).alias("next_event_event_at"),
    )
)

labeled = (
    windowed
    .filter(F.col("actor").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_at_month",
        "event_id",
        "event_actor",
        "last_event_event_at",
        "next_event_event_at",
        F.concat_ws("-", F.col("actor"), F.col("event_actor")).alias("actor_event_actor_label"),
        F.when(F.col("event_actor").isin("customer", "agent"), "group_a").when(F.col("event_actor") == "system", "group_b").otherwise("other").alias("event_actor_group"),
        F.substring(F.trim(F.col("event_actor")), 1, 2).alias("event_actor_prefix"),
    )
)

ranked = (
    labeled
    .select(
        "*",
        F.rank().over(w_event_type_event_actor_by_ne).alias("next_event_event_at_rank"),
        F.dense_rank().over(w_actor_ticket_id_by_last_even).alias("last_event_event_at_dense_rank"),
        F.lead("event_at_month", 2).over(w_event_type_event_actor_by_ne).alias("next2_event_at_month"),
    )
)

per_group = (
    ranked
    .filter(F.month(F.col("event_at_month")).isin(2, 3))
    .groupBy("last_event_event_at_dense_rank", "event_actor_group")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("event_actor_group").alias("distinct_event_actor_groups"),
        F.max("last_event_event_at_dense_rank").alias("max_last_event_event_at_dense_rank"),
        F.round(F.stddev("next_event_event_at_rank"), 2).alias("stddev_next_event_event_at_rank"),
        F.round(100 * F.count(F.when(F.col("actor").isNotNull(), 1)) / F.count("*"), 1).alias("pct_actor_is_not_null"),
        F.sum("last_event_event_at_dense_rank").alias("total_last_event_event_at_dense_rank"),
    )
    .filter(F.col("n_rows") >= 2)
)

with_trends = (
    per_group
    .select(
        "*",
        F.ntile(5).over(w_last_event_event_at_dense_ra).alias("total_last_event_event_at_dense_rank_quintile"),
        F.nth_value("pct_actor_is_not_null", 2).over(w_last_event_event_at_dense_ra_2).alias("second_pct_actor_is_not_null"),
        F.max("n_rows").over(w_distinct_event_actor_groups).alias("distinct_event_actor_groups_max_n_rows"),
        F.row_number().over(w_event_actor_group_last_event).alias("seq_no"),
        F.round(F.percent_rank().over(w_last_event_event_at_dense_ra_3), 3).alias("total_last_event_event_at_dense_rank_percent_ran"),
    )
)

result = (
    with_trends
    .groupBy("n_rows")
    .pivot("event_actor_group", ["group_b", "other"])
    .agg(F.count("max_last_event_event_at_dense_rank"))
    .orderBy("n_rows")
)
