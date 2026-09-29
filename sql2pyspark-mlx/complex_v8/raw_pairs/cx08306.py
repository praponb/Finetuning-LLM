from pyspark.sql import functions as F

base = (
    df_tickets
    .filter(F.col("priority") == "P4")
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
    )
)

result = (
    base
    .groupBy("agent_id")
    .pivot("priority", ["P4", "P3"])
    .agg(F.sum("requester_id_per_csat").alias("total"), F.count("requester_id_per_csat").alias("n"))
    .orderBy("agent_id")
)
