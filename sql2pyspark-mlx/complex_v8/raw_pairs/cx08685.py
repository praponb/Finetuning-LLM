from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_agent_tier_by_agent = Window.partitionBy("agent_id", "agent_tier").orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_agent_tier_by_agent_2 = Window.partitionBy("agent_id", "agent_tier").orderBy(F.col("agent_hired_on").desc(), "id")
w_priority_by_closed_at_full = Window.partitionBy("priority").orderBy(F.col("closed_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_agent_id_by_agent_h = Window.partitionBy("priority", "agent_id").orderBy(F.col("agent_hired_on").desc(), "id")
w_agent_id_by_agent_hired_on = Window.partitionBy("agent_id").orderBy("agent_hired_on", "id")
w_agent_hired_on_month_by_clos = Window.partitionBy(F.month("agent_hired_on")).orderBy("closed_at", "id")
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_priority_agent_id_by_agent_h_2 = Window.partitionBy("priority", "agent_id").orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_agent_id_by_agent_h_3 = Window.partitionBy("priority", "agent_id").orderBy(F.col("agent_hired_on").desc())

cleaned_tickets = (
    df_tickets
    .filter(F.col("requester_id") < 22)
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
    )
)

combined_details_agent = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.tags",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

windowed = (
    combined_details_agent
    .select(
        "*",
        F.nth_value("closed_at", 2).over(w_agent_id_agent_tier_by_agent).alias("second_closed_at"),
        F.rank().over(w_agent_id_agent_tier_by_agent_2).alias("agent_hired_on_rank"),
        F.round(F.cume_dist().over(w_agent_id_agent_tier_by_agent_2), 3).alias("agent_hired_on_cume_dist"),
        F.nth_value("agent_hired_on", 2).over(w_priority_by_closed_at_full).alias("second_agent_hired_on"),
        F.lag("agent_hired_on").over(w_priority_agent_id_by_agent_h).alias("prev_agent_hired_on"),
        F.rank().over(w_agent_id_by_agent_hired_on).alias("agent_hired_on_rank_2"),
        F.row_number().over(w_agent_id_agent_tier_by_agent_2).alias("agent_hired_on_seq"),
        F.dense_rank().over(w_agent_id_by_agent_hired_on).alias("agent_hired_on_dense_rank"),
        F.lag("closed_at", 3).over(w_agent_hired_on_month_by_clos).alias("prev3_closed_at"),
        F.ntile(5).over(w_all_by_closed_at).alias("closed_at_quintile"),
        F.first("agent_hired_on", ignorenulls=True).over(w_priority_agent_id_by_agent_h_2).alias("first_known_agent_hired_on"),
        F.rank().over(w_priority_agent_id_by_agent_h_3).alias("agent_hired_on_rank_3"),
    )
)

leaders = (
    windowed
    .filter(F.col("agent_hired_on_rank_3") <= 2)
    .drop("agent_hired_on_rank_3")
)

result = (
    leaders
    .filter(F.col("agent_hired_on_rank_2") >= 3)
    .select(
        "id",
        "closed_at",
        "priority",
        "agent_team",
        "agent_tier",
        "second_closed_at",
        "agent_hired_on_rank",
        "agent_hired_on_cume_dist",
        "second_agent_hired_on",
        "prev_agent_hired_on",
        "agent_hired_on_rank_2",
        "prev3_closed_at",
    )
    .orderBy(F.col("agent_hired_on_rank_2").desc())
)
