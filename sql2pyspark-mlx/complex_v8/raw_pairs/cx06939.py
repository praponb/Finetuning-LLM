from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_group = Window.partitionBy("actor_group")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").asc_nulls_last())
w_all_by_event_at = Window.orderBy(F.col("event_at").asc_nulls_last(), "id")

scoped = (
    df_ticket_events
    .filter(F.col("event_type") == "escalated")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.when(F.col("actor").isin("system", "customer"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)

labeled = (
    scoped
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_type_clean",
        "actor_group",
        F.concat_ws("-", F.col("actor_group"), F.col("event_type")).alias("actor_group_event_type_label"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

enriched = (
    labeled
    .filter(F.upper(F.trim(F.col("event_type_clean"))).like("C%"))
    .withColumn("event_at_month", F.month(F.col("event_at")))
)

ranked = (
    enriched
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_type_clean",
        "actor_group",
        "actor_group_event_type_label",
        "actor_slug",
        "event_at_month",
        F.size(F.collect_set("event_type").over(w_actor_group)).alias("distinct_event_type_in_actor_group"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
    )
)

result = (
    ranked
    .select(
        "id",
        "event_at",
        "actor",
        "event_type_clean",
        "actor_group_event_type_label",
        "event_at_month",
        "distinct_event_type_in_actor_group",
        F.initcap(F.trim(F.col("actor_slug"))).alias("actor_slug_clean"),
    )
    .orderBy(F.col("actor").asc_nulls_last(), F.col("event_at_month").desc(), "id")
    .limit(20)
)
