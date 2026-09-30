from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .select(
        F.col("agent_id").alias("ref_id"),
        "id",
        F.lit("ticket").alias("record_type"),
    )
)

ticket_rows = (
    df_tickets
    .select(
        F.col("csat").alias("ref_id"),
        F.col("agent_id").alias("id"),
        F.lit("ticket_2").alias("record_type"),
    )
)

ticket_side_2 = (
    df_tickets
    .filter((F.col("channel").isNotNull()) & (F.col("priority") == "P1"))
    .select(
        F.col("requester_id").alias("ref_id"),
        F.col("csat").alias("id"),
        F.lit("ticket_3").alias("record_type"),
    )
)

result = (
    ticket_side
    .unionByName(ticket_rows)
    .unionByName(ticket_side_2)
    .orderBy(F.col("ref_id").desc(), "record_type")
)
