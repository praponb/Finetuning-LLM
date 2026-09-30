from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_priority_p4_agent_hired_on = Window.partitionBy("n_priority_p4", F.year("agent_hired_on_month")).orderBy(F.col("n_priority_in_p3_p1").desc(), "agent_hired_on_month", "closed_at_year", "channel")
w_all_by_n_priority_in_p3_p1 = Window.orderBy("n_priority_in_p3_p1", "agent_hired_on_month", "closed_at_year", "channel")
w_closed_at_year_n_priority_p4 = Window.partitionBy("closed_at_year", "n_priority_p4")
w_closed_at_year_n_priority_p4_2 = Window.partitionBy("closed_at_year", "n_priority_p4").orderBy("n_priority_p4", "agent_hired_on_month", "closed_at_year", "channel")

filtered = (
    df_tickets
    .filter((F.col("channel") != "phone") | (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

with_details = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.days_since_closed_at",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

totals = (
    with_details
    .groupBy(
        F.date_trunc("month", "agent_hired_on").alias("agent_hired_on_month"),
        F.year("closed_at").alias("closed_at_year"),
        "channel",
    )
    .agg(
        F.count(F.when(F.col("priority") == "P4", 1)).alias("n_priority_p4"),
        F.count(F.when(F.col("priority").isin("P3", "P1"), 1)).alias("n_priority_in_p3_p1"),
    )
)

sequenced = (
    totals
    .select(
        "*",
        F.rank().over(w_n_priority_p4_agent_hired_on).alias("n_priority_in_p3_p1_rank"),
        F.row_number().over(w_all_by_n_priority_in_p3_p1).alias("rn"),
        F.count(F.lit(1)).over(w_closed_at_year_n_priority_p4).alias("rows_in_closed_at_year_n_priority_p4"),
        F.lead("n_priority_in_p3_p1").over(w_closed_at_year_n_priority_p4_2).alias("next_n_priority_in_p3_p1"),
    )
)

enriched = (
    sequenced
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
        F.date_format(F.col("agent_hired_on_month"), "yyyy-MM-dd HH:00").alias("agent_hired_on_month_label"),
        F.date_trunc("hour", F.col("agent_hired_on_month")).alias("agent_hired_on_month_hour"),
    )
)

enriched_2 = (
    enriched
    .filter(F.col("next_n_priority_in_p3_p1") >= 2)
    .select(
        "*",
        F.abs(F.col("closed_at_year") - 2025).alias("closed_at_year_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug_2"),
        F.abs(F.col("n_priority_in_p3_p1") - 3).alias("n_priority_in_p3_p1_distance"),
    )
)

result = (
    enriched_2
    .groupBy("n_priority_in_p3_p1_rank", "n_priority_p4")
    .pivot("channel", ["email", "chat", "phone", "web"])
    .agg(F.max("next_n_priority_in_p3_p1"))
    .orderBy("n_priority_in_p3_p1_rank", "n_priority_p4")
)
