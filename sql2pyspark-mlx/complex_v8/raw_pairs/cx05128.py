from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .filter((F.col("priority") == "P1") & (F.col("requester_id") <= 22))
    .select(
        F.col("channel").alias("label"),
        "priority",
        F.col("opened_at").alias("happened_at"),
    )
)

ticket_side = (
    df_tickets
    .filter(F.col("csat") > 2)
    .select(
        F.col("priority").alias("label"),
        F.col("channel").alias("priority"),
        F.col("opened_at").alias("happened_at"),
    )
)

result = (
    ticket_rows
    .unionByName(ticket_side)
    .orderBy(F.col("priority").desc(), "happened_at")
)
