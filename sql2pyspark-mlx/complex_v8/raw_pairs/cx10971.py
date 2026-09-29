from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_ticket_opened_at_to_dat = Window.partitionBy(F.to_date("last_ticket_opened_at"), "agent_id").orderBy(F.col("last_ticket_opened_at").desc_nulls_first(), "id")
w_last_ticket_opened_at_to_dat_2 = Window.partitionBy(F.to_date("last_ticket_opened_at"), "agent_id").orderBy(F.col("last_ticket_opened_at").desc_nulls_first())

base = (
    df_tickets
    .filter((F.col("requester_id") >= 8) & (F.col("channel").isNotNull()))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.least(F.col("requester_id"), F.col("csat")).alias("least_requester_id_csat"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.max("opened_at").alias("last_ticket_opened_at"))
)

with_counts = (
    base.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.last_ticket_opened_at")
)

with_trends = (
    with_counts
    .select(
        "*",
        F.row_number().over(w_last_ticket_opened_at_to_dat).alias("seq_no"),
        F.round(F.cume_dist().over(w_last_ticket_opened_at_to_dat_2), 3).alias("last_ticket_opened_at_cume_dist"),
        F.dense_rank().over(w_last_ticket_opened_at_to_dat_2).alias("last_ticket_opened_at_dense_rank"),
    )
)

result = (
    with_trends
    .groupBy("csat", "closed_at")
    .pivot("channel", ["email", "chat", "phone"])
    .agg(F.max("least_requester_id_csat"))
    .orderBy("csat", "closed_at")
)
