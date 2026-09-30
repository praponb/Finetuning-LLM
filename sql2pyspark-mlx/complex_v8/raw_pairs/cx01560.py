from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_ids = Window.partitionBy("distinct_ids")
w_n_event_type_not_in_reopened = Window.partitionBy("n_event_type_not_in_reopened_e", "distinct_actors").orderBy(F.col("distinct_ids").desc())
w_all_by_last_event_at = Window.orderBy("last_event_at", "actor", "ticket_id", "event_type")

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

summary = (
    cleaned
    .groupBy("actor", "ticket_id", "event_type")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count(F.when(~F.col("event_type").isin("reopened", "escalated"), 1)).alias("n_event_type_not_in_reopened_e"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.round(100 * F.count(F.when(F.col("event_type") == "created", 1)) / F.count("*"), 1).alias("pct_event_type_created"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("actor").alias("distinct_actors"),
    )
)

windowed = (
    summary
    .select(
        "actor",
        "ticket_id",
        "event_type",
        "last_event_at",
        "pct_event_type_created",
        "distinct_actors",
        F.sum("pct_event_type_created").over(w_distinct_ids).alias("distinct_ids_sum_pct_event_type_created"),
        F.rank().over(w_n_event_type_not_in_reopened).alias("distinct_ids_rank"),
        F.row_number().over(w_all_by_last_event_at).alias("last_event_at_seq"),
    )
)

top_rows = windowed.filter((F.col("last_event_at_seq") == 1) & (F.col("ticket_id") >= 19))

derived = (
    top_rows
    .select(
        "*",
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
        F.when(F.col("event_type").isin("created", "escalated"), "group_a").when(F.col("event_type") == "assigned", "group_b").otherwise("other").alias("event_type_group"),
    )
)

result = (
    derived
    .groupBy("distinct_ids_rank", "event_type")
    .pivot("actor_group", ["other", "group_b", "group_a"])
    .agg(F.sum("distinct_ids_sum_pct_event_type_created").alias("amount"), F.max("distinct_ids_sum_pct_event_type_created").alias("max_v"))
    .orderBy("distinct_ids_rank", "event_type")
)
