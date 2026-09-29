from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat = Window.partitionBy("csat")
w_all_by_opened_at_running = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")

prepared = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

ranked = (
    prepared
    .select(
        "*",
        F.max("requester_id").over(w_csat).alias("csat_max_requester_id"),
        F.min("requester_id").over(w_all_by_opened_at_running).alias("min_requester_id_so_far"),
        F.count(F.lit(1)).over(w_csat).alias("rows_in_csat"),
        F.max("requester_id").over(w_all_by_opened_at_running).alias("max_requester_id_so_far"),
        F.round(F.percent_rank().over(w_all_by_opened_at), 3).alias("opened_at_percent_rank"),
        F.min("csat").over(w_csat).alias("csat_min_csat"),
    )
)

with_details_agent = (
    ranked.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.length(F.trim(F.col("su.agent_name"))) >= 6)
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.csat_max_requester_id",
        "s.min_requester_id_so_far",
        "s.rows_in_csat",
        "s.max_requester_id_so_far",
        "s.csat_min_csat",
        "su.agent_name",
    )
)

result = (
    with_details_agent
    .groupBy("closed_at", "opened_at")
    .pivot("channel", ["chat", "email", "web", "phone"])
    .agg(F.avg("csat_max_requester_id"))
    .orderBy("closed_at", "opened_at")
)
