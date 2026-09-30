from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter(F.col("event_type").isin("assigned", "created"))
    .select(
        F.lit("event").alias("src"),
        F.col("ticket_id").alias("ref_id"),
        F.col("actor").alias("category"),
    )
)

agent_rows = (
    df_support_agents
    .select(
        F.lit("agent").alias("src"),
        F.col("id").alias("ref_id"),
        F.col("team").alias("category"),
    )
)

all_events = (
    event_side
    .unionByName(agent_rows)
    .distinct()
)

result = (
    all_events
    .groupBy("src")
    .agg(F.count("*").alias("n_rows"), F.sum("ref_id").alias("total_ref_id"))
    .orderBy("src")
)
