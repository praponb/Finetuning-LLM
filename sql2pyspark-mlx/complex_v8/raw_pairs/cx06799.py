from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_agent_id_channel_by_requeste = Window.partitionBy("agent_id", "channel").orderBy(F.col("requester_id").asc_nulls_last(), "id")

scoped = (
    df_tickets
    .filter(F.month(F.col("opened_at")).isin(1, 2))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "tags",
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
        F.date_format(F.col("closed_at"), "yyyy-MM-dd HH:00").alias("closed_at_label"),
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
    )
)

qualified = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.size(F.col("t.tags")) >= 1) & (F.col("t.channel") == "web"), "left_semi")
)

qualified_2 = (
    qualified
    .crossJoin(qualified.agg(F.avg("requester_id").alias("overall_avg_requester_id")))
    .filter(F.col("requester_id") > F.col("overall_avg_requester_id"))
    .drop("overall_avg_requester_id")
)

metrics = (
    qualified_2
    .select(
        "id",
        "requester_id",
        "opened_at",
        "priority",
        "closed_at_label",
        "closed_at_date",
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id), 2).alias("requester_id_vs_agent_id_avg"),
        F.round(F.col("csat_plus_requester_id") / F.nullif(F.sum("csat_plus_requester_id").over(w_agent_id), F.lit(0)), 4).alias("csat_plus_requester_id_share_of_agent_id"),
        F.lag("opened_at", 2).over(w_agent_id_channel_by_requeste).alias("prev2_opened_at"),
        F.round(F.col("csat_plus_requester_id") - F.avg("csat_plus_requester_id").over(w_agent_id), 2).alias("csat_plus_requester_id_vs_agent_id_avg"),
    )
)

result = (
    metrics
    .select(
        "id",
        "opened_at",
        "closed_at_label",
        "closed_at_date",
        "requester_id_vs_agent_id_avg",
        "prev2_opened_at",
        "csat_plus_requester_id_vs_agent_id_avg",
        F.concat(F.col("closed_at_label"), F.lit("#"), F.col("requester_id").cast("string")).alias("closed_at_label_ref"),
    )
    .orderBy(F.col("csat_plus_requester_id_vs_agent_id_avg").asc())
)
