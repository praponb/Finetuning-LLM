from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_min_csat_moving_6 = Window.orderBy(F.col("min_csat").desc_nulls_first(), "channel", "agent_id").rowsBetween(-6, Window.currentRow)
w_min_csat_n_priority_p2 = Window.partitionBy("min_csat", "n_priority_p2")

cleaned_tickets = (
    df_tickets
    .filter((F.col("closed_at") < F.lit("2025-02-20 15:00:00").cast("timestamp")) | (F.col("priority") == "P2"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.when(F.col("requester_id") >= 21, "A").when(F.col("requester_id") >= 8, "B").otherwise("C").alias("requester_id_tier"),
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

summary = (
    cleaned_tickets
    .groupBy("channel", "agent_id")
    .agg(
        F.sum(F.when(F.col("priority") == "P2", 1).otherwise(0)).alias("n_priority_p2"),
        F.min("csat").alias("min_csat"),
    )
)

with_trends = (
    summary
    .select(
        "*",
        F.sum("min_csat").over(w_all_by_min_csat_moving_6).alias("min_csat_sum_7"),
        F.max("min_csat").over(w_min_csat_n_priority_p2).alias("min_csat_n_priority_p2_max_min_csat"),
    )
)

labeled = (
    with_trends
    .select(
        "*",
        F.when(F.col("channel").isin("phone", "chat"), "group_a").when(F.col("channel") == "web", "group_b").otherwise("other").alias("channel_group"),
    )
)

filtered = (
    labeled.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

result = (
    filtered
    .groupBy("min_csat")
    .pivot("channel", ["email", "phone"])
    .agg(F.sum("min_csat_sum_7"))
)
