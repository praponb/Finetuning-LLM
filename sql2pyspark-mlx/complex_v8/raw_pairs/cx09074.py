from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_by_opened_at_full = Window.partitionBy("csat").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat = Window.partitionBy("csat")

valid_tickets = df_tickets.filter(F.col("channel") == "chat")

ranked = (
    valid_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
        F.nth_value("csat", 2).over(w_csat_by_opened_at_full).alias("second_csat"),
        F.max("csat").over(w_csat).alias("csat_max_csat"),
        F.sum("requester_id").over(w_csat).alias("csat_sum_requester_id"),
    )
)

agent_id_stats = (
    ranked
    .groupBy("agent_id")
    .agg(F.percentile_approx("csat_max_csat", 0.5).alias("median_csat_max_csat"))
)

vs_group = (
    ranked.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.csat_max_csat") >= F.col("g.median_csat_max_csat"))
    .select(
        "s.*",
        "g.median_csat_max_csat",
        F.round(F.col("s.csat_max_csat") - F.col("g.median_csat_max_csat"), 2).alias("csat_max_csat_minus_median"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
        "second_csat",
        "csat_max_csat",
        "csat_sum_requester_id",
        "median_csat_max_csat",
        "csat_max_csat_minus_median",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 60, 1).alias("minutes_closed_at_to_opened_at"),
    )
    .orderBy(F.col("csat_max_csat_minus_median").desc())
)
