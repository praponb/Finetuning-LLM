from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .filter((F.col("opened_at") < F.lit("2025-03-02 08:00:00").cast("timestamp")) & (F.col("priority") == "P2"))
    .select("priority", "closed_at", F.lit("ticket").alias("record_type"))
)

ticket_side = (
    df_tickets
    .filter((F.hour(F.col("closed_at")).between(9, 13)) & (F.col("agent_id") <= 9))
    .select(
        F.col("channel").alias("priority"),
        F.col("opened_at").alias("closed_at"),
        F.lit("ticket_2").alias("record_type"),
    )
)

result = (
    ticket_rows
    .unionByName(ticket_side)
    .orderBy(F.col("closed_at").desc())
)
