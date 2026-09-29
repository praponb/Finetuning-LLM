from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_ticket_closed_at_year = Window.partitionBy(F.year("first_ticket_closed_at"))
w_priority_len_by_priority_len = Window.partitionBy("priority_len").orderBy("priority_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_len_by_priority_len_2 = Window.partitionBy("priority_len").orderBy("priority_len", "id")
w_channel_by_first_ticket_clos = Window.partitionBy("channel").orderBy(F.col("first_ticket_closed_at_year_min_priority_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_dayofweek_priority = Window.partitionBy("closed_at_dayofweek", "priority_len").orderBy("priority_len_dense_rank", "id")
w_channel_by_first_ticket_clos_2 = Window.partitionBy("channel").orderBy(F.col("first_ticket_closed_at_year_min_priority_len").desc())

recent_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.dayofweek(F.col("closed_at")).alias("closed_at_dayofweek"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.min("closed_at").alias("first_ticket_closed_at"))
)

profiled = (
    recent_tickets.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.first_ticket_closed_at")
)

with_trends = (
    profiled
    .select(
        "id",
        "channel",
        "tags",
        "closed_at_dayofweek",
        "priority_len",
        "first_ticket_closed_at",
        F.min("priority_len").over(w_first_ticket_closed_at_year).alias("first_ticket_closed_at_year_min_priority_len"),
        F.first("closed_at", ignorenulls=True).over(w_priority_len_by_priority_len).alias("first_known_closed_at"),
        F.dense_rank().over(w_priority_len_by_priority_len_2).alias("priority_len_dense_rank"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.first("priority_len_dense_rank", ignorenulls=True).over(w_channel_by_first_ticket_clos).alias("first_known_priority_len_dense_rank"),
        F.lead("priority_len").over(w_closed_at_dayofweek_priority).alias("next_priority_len"),
        F.dense_rank().over(w_channel_by_first_ticket_clos_2).alias("first_ticket_closed_at_year_min_priority_len_den"),
    )
)

result = (
    ranked
    .rollup("channel", "first_known_priority_len_dense_rank")
    .agg(
        F.grouping("channel").alias("is_total_channel"),
        F.sum(F.when(F.col("channel") == "email", F.col("first_ticket_closed_at_year_min_priority_len_den")).otherwise(0)).alias("first_ticket_closed_at_year_min_priority_len_den_2"),
        F.count("*").alias("row_count"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
)
