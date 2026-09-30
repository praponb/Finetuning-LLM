from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at = Window.orderBy("closed_at", "id")
w_all_by_closed_at_ties = Window.orderBy("closed_at")
w_all_by_closed_at_running = Window.orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_by_closed_at = Window.partitionBy("priority").orderBy("closed_at", "id")
w_priority_by_closed_at_full = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_priority_by_closed = Window.partitionBy("agent_id", "priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_priority_by_closed_2 = Window.partitionBy("agent_id", "priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_by_closed_at_ties = Window.partitionBy("priority").orderBy("closed_at")

cleaned_tickets = (
    df_tickets
    .filter((F.col("opened_at") < F.lit("2025-03-08 11:00:00").cast("timestamp")) & (F.col("csat") <= 4))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
    )
)

joined_agent = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.upper(F.trim(F.col("su.agent_name"))).like("L%"))
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "su.agent_name",
    )
)

windowed = (
    joined_agent
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_closed_at), 3).alias("closed_at_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_closed_at_ties), 3).alias("closed_at_percent_rank_2"),
        F.last("closed_at", ignorenulls=True).over(w_all_by_closed_at_running).alias("last_known_closed_at"),
        F.lead("closed_at").over(w_all_by_closed_at).alias("next_closed_at"),
        F.lead("closed_at", 2).over(w_priority_by_closed_at).alias("next2_closed_at"),
        F.row_number().over(w_priority_by_closed_at).alias("rn"),
        F.ntile(5).over(w_all_by_closed_at).alias("closed_at_quintile"),
        F.nth_value("closed_at", 2).over(w_priority_by_closed_at_full).alias("second_closed_at"),
        F.last("closed_at", ignorenulls=True).over(w_agent_id_priority_by_closed).alias("last_known_closed_at_2"),
        F.nth_value("closed_at", 2).over(w_agent_id_priority_by_closed_2).alias("second_closed_at_2"),
        F.rank().over(w_priority_by_closed_at_ties).alias("closed_at_rank"),
    )
)

first_rows = (
    windowed
    .filter(F.col("rn") <= 3)
    .drop("rn")
)

result = (
    first_rows
    .select(
        "id",
        "agent_id",
        "closed_at",
        "agent_name",
        "closed_at_percent_rank_2",
        "last_known_closed_at",
        "next_closed_at",
        "closed_at_quintile",
        "second_closed_at",
        "last_known_closed_at_2",
        "closed_at_rank",
    )
    .orderBy("closed_at")
)
