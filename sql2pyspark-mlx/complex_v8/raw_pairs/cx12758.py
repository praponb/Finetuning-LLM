from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), F.col("id").desc())

filtered_tickets = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 2) & (F.col("channel") == "phone"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "tags",
    )
)

deduped = (
    filtered_tickets
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

without_matches = (
    deduped.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.month(F.col("t.event_at")).isin(1, 3)) & (F.col("t.event_type") != "replied"), "left_anti")
)

rollup_base = (
    without_matches
    .groupBy(F.to_date("opened_at").alias("opened_at_day"), "agent_id")
    .agg(
        F.sum("requester_id").alias("total_requester_id"),
        F.max("opened_at").alias("last_opened_at"),
        F.count("*").alias("n_rows"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.percentile_approx("requester_id", 0.5).alias("median_requester_id"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    rollup_base
    .groupingSets(
        [["agent_id", "distinct_agent_ids"], ["agent_id"], []],
        "agent_id", "distinct_agent_ids",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("opened_at_day").alias("last_opened_at_day"),
        F.countDistinct("agent_id").alias("distinct_agent_ids_2"),
        F.datediff(F.max("opened_at_day"), F.min("opened_at_day")).alias("opened_at_day_span_days"),
    )
)
