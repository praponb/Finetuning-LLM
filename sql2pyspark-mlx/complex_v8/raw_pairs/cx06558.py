from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_requester_id_pri = Window.partitionBy("agent_id").orderBy(F.col("requester_id").desc(), "id").rowsBetween(-1, -1)
w_agent_id_by_requester_id = Window.partitionBy("agent_id").orderBy(F.col("requester_id").desc(), "id")
w_agent_id_by_requester_id_pri_2 = Window.partitionBy("agent_id").orderBy(F.col("requester_id").desc(), "id").rowsBetween(-2, -1)

cleaned_tickets = (
    df_tickets
    .filter((F.col("channel").isNotNull()) | (F.col("csat") > 1))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        F.date_trunc("year", F.col("closed_at")).alias("closed_at_year"),
    )
)

windowed = (
    cleaned_tickets
    .select(
        "*",
        F.round(F.avg("csat").over(w_agent_id_by_requester_id_pri), 2).alias("csat_avg_1_prior"),
        F.ntile(10).over(w_agent_id_by_requester_id).alias("requester_id_decile"),
        F.round(F.cume_dist().over(w_agent_id_by_requester_id), 3).alias("requester_id_cume_dist"),
        F.round(F.avg("requester_id").over(w_agent_id_by_requester_id_pri_2), 2).alias("requester_id_avg_2_prior"),
    )
)

qualified = (
    windowed
    .crossJoin(windowed.agg(F.avg("requester_id_cume_dist").alias("overall_avg_requester_id_cume_dist")))
    .filter(F.col("requester_id_cume_dist") > F.col("overall_avg_requester_id_cume_dist"))
    .drop("overall_avg_requester_id_cume_dist")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.max("closed_at").alias("last_ticket_closed_at"))
)

summarized_rows = (
    qualified.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.last_ticket_closed_at")
)

result = (
    summarized_rows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "closed_at_year",
        "csat_avg_1_prior",
        "requester_id_decile",
        "requester_id_cume_dist",
        "requester_id_avg_2_prior",
        "last_ticket_closed_at",
        F.concat(F.col("priority"), F.lit("#"), F.col("requester_id").cast("string")).alias("priority_ref"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
    .orderBy(F.col("closed_at").desc_nulls_first(), "closed_at_year", "id")
    .limit(20)
)
