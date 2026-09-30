from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("event_type", "actor").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_actor_by_ticket_requester_id = Window.partitionBy("actor").orderBy("ticket_requester_id")
w_actor_by_ticket_requester_id_2 = Window.partitionBy("actor").orderBy("ticket_requester_id", "ticket_id", "actor")
w_distinct_ticket_ids = Window.partitionBy("distinct_ticket_ids")

filtered_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")) & (F.col("event_type") != "replied"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

latest = (
    filtered_events
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

grouped = (
    latest
    .groupBy("ticket_id", "actor")
    .agg(
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
    )
)

combined_details_ticket = (
    grouped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select("s.*", F.col("t.requester_id").alias("ticket_requester_id"))
)

metrics = (
    combined_details_ticket
    .select(
        "ticket_id",
        "actor",
        "actor_list",
        "ticket_requester_id",
        F.rank().over(w_actor_by_ticket_requester_id).alias("ticket_requester_id_rank"),
        F.round(F.percent_rank().over(w_actor_by_ticket_requester_id_2), 3).alias("ticket_requester_id_percent_rank"),
        F.round(F.cume_dist().over(w_actor_by_ticket_requester_id), 3).alias("ticket_requester_id_cume_dist"),
        F.count(F.lit(1)).over(w_distinct_ticket_ids).alias("rows_in_distinct_ticket_ids"),
    )
)

result = (
    metrics
    .select(
        "ticket_id",
        "actor",
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
        F.when(F.col("actor").isin("system", "agent"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group_2"),
    )
    .orderBy(F.col("actor_group").asc_nulls_last())
)
