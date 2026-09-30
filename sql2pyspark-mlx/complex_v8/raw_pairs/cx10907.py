from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_agent_id_by_csat = Window.partitionBy("csat", "agent_id").orderBy(F.col("csat").desc(), "id")
w_csat_agent_id_by_csat_full = Window.partitionBy("csat", "agent_id").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_agent_id_by_csat_ties = Window.partitionBy("csat", "agent_id").orderBy(F.col("csat").desc())
w_csat = Window.partitionBy("csat")

cleaned = (
    df_tickets
    .filter((F.col("priority").isNotNull()) & (F.col("csat") >= 2))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("priority"), F.lit("#"), F.col("agent_id").cast("string")).alias("priority_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.ntile(5).over(w_csat_agent_id_by_csat).alias("csat_quintile"),
        F.last("closed_at").over(w_csat_agent_id_by_csat_full).alias("last_closed_at"),
        F.round(F.percent_rank().over(w_csat_agent_id_by_csat_ties), 3).alias("csat_percent_rank"),
        F.max("csat").over(w_csat).alias("csat_max_csat"),
    )
)

result = (
    with_windows
    .groupBy("last_closed_at", "agent_id")
    .pivot("channel", ["chat", "web"])
    .agg(F.sum("csat_max_csat"))
    .orderBy("last_closed_at", "agent_id")
)
