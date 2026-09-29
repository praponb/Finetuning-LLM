from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("priority").orderBy(F.col("closed_at").desc(), F.col("id").desc())

scoped_tickets = (
    df_tickets
    .filter(F.to_date(F.col("closed_at")) < F.lit("2025-02-04").cast("date"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("priority"), F.lit("#"), F.col("csat").cast("string")).alias("priority_ref"),
        F.when(F.col("csat") >= 4, "large").when(F.col("csat") >= 2, "medium").otherwise("small").alias("csat_tier"),
    )
)

deduped = (
    scoped_tickets
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_activity = (
    deduped.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (~F.col("t.priority").isin("P1", "P2")) & (F.size(F.col("t.tags")) >= 2), "left_anti")
)

result = (
    with_activity
    .filter(F.col("channel").isin("web", "phone", "chat"))
    .select(
        "id",
        "requester_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "priority_ref",
        "csat_tier",
        F.abs(F.col("requester_id") - 18).alias("requester_id_distance"),
    )
)
