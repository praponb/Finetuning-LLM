from pyspark.sql import functions as F

agent_side = df_support_agents.select(F.col("tier").alias("label"), F.col("team").alias("category"))

event_side = df_ticket_events.select("actor", "event_type")

stacked = agent_side.union(event_side)

result = (
    stacked
    .groupBy("label")
    .agg(F.count("*").alias("n_rows"))
    .orderBy("label")
)
