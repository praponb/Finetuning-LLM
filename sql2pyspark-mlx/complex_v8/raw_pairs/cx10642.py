from pyspark.sql import functions as F

agent_side = df_support_agents.select("id", F.col("team").alias("category"))

event_rows = (
    df_ticket_events
    .filter((~F.col("event_type").isin("created", "resolved")) & (F.to_date(F.col("event_at")) < F.lit("2025-02-24").cast("date")))
    .select("id", F.col("actor").alias("category"))
)

stacked = (
    agent_side
    .unionByName(event_rows)
    .distinct()
)

result = (
    stacked
    .groupBy("category")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("category")
)
