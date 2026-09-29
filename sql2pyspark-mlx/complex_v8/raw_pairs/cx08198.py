from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("channel", "agent_id").orderBy(F.col("csat").desc(), F.col("id").desc())

base_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

latest = (
    base_tickets
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

without_matches = (
    latest.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.when(F.col("requester_id") >= 23, "large").when(F.col("requester_id") >= 7, "medium").otherwise("small").alias("requester_id_tier"),
        F.when(F.col("priority").isin("P2", "P3"), "group_a").when(F.col("priority") == "P4", "group_b").otherwise("other").alias("priority_group"),
    )
)
