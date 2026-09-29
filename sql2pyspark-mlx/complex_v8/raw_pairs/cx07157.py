from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_ticket_events
    .filter((F.col("actor") == "customer") & (F.col("ticket_id") <= 57))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.first("event_at").over(w_ticket_id_event_type_by_even).alias("first_event_at"),
        F.lag("event_at").over(w_ticket_id_event_type_by_even_2).alias("prev_event_at"),
        F.last("event_at").over(w_ticket_id_event_type_by_even).alias("last_event_at"),
        F.last("event_at").over(w_event_type_by_event_at_full).alias("last_event_at_2"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_slug",
        "first_event_at",
        "prev_event_at",
        "last_event_at",
        "last_event_at_2",
        F.concat_ws("-", F.col("actor_slug"), F.col("event_type")).alias("actor_slug_event_type_label"),
        F.hour(F.col("first_event_at")).alias("first_event_at_hour"),
        F.initcap(F.trim(F.col("actor_slug"))).alias("actor_slug_clean"),
    )
)

scored = (
    enriched
    .filter(F.col("actor_slug_event_type_label").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor_slug",
        "first_event_at",
        "prev_event_at",
        "last_event_at",
        "last_event_at_2",
        "actor_slug_event_type_label",
        "first_event_at_hour",
        "actor_slug_clean",
        F.when(F.col("event_type").isin("replied", "assigned"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.concat(F.col("actor_slug_clean"), F.lit("#"), F.col("id").cast("string")).alias("actor_slug_clean_ref"),
    )
)

result = (
    scored
    .rollup("event_type", "first_event_at_hour", "event_type_group")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("n_rows"),
        F.percentile_approx("first_event_at_hour", 0.5).alias("median_first_event_at_hour"),
    )
    .select(
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "first_event_at_hour",
        F.coalesce(F.col("event_type_group"), F.lit("ALL")).alias("event_type_group"),
        "grouping_level",
        "n_rows",
        "median_first_event_at_hour",
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("first_event_at_hour").asc_nulls_last(),
        F.col("event_type_group").asc_nulls_last(),
    )
)
