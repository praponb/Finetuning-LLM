from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id").orderBy(F.col("closed_at_quarter").desc(), F.col("id").desc())

base_tickets = (
    df_tickets
    .filter((F.col("opened_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")) & (F.col("closed_at") < F.lit("2025-02-27 06:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.concat_ws(": ", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.quarter(F.col("closed_at")).alias("closed_at_quarter"),
    )
)

without_matches = (
    base_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.closed_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("priority_channel_label").isNotNull())
)

latest = (
    without_matches
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

labeled = (
    latest
    .select(
        "*",
        F.least(F.col("requester_id"), F.col("closed_at_quarter")).alias("least_requester_id_closed_at_quarter"),
        F.abs(F.col("requester_id") - 16).alias("requester_id_distance"),
        F.date_trunc("month", F.col("closed_at")).alias("closed_at_month"),
    )
)

result = (
    labeled
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        "priority_channel_label",
        "closed_at_quarter",
        "least_requester_id_closed_at_quarter",
        "closed_at_month",
        F.date_format(F.col("closed_at"), "yyyy-MM").alias("closed_at_label"),
        F.when(F.col("channel").isin("email", "chat"), "group_a").when(F.col("channel") == "phone", "group_b").otherwise("other").alias("channel_group"),
    )
)
