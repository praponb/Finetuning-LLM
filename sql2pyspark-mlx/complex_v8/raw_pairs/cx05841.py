from pyspark.sql import functions as F

base = (
    df_tickets
    .filter(F.col("priority").isNotNull())
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
        F.concat_ws(": ", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.initcap(F.trim(F.col("channel"))).alias("channel_clean"),
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

tagged = (
    base
    .filter(F.col("priority_channel_label").isNotNull())
    .withColumn("requester_id_per_csat", F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3))
)

result = (
    tagged
    .select(
        "id",
        F.col("requester_id").alias("requester_id_v"),
        F.col("csat").alias("csat_v"),
    )
    .unpivot(
        ids=["id"],
        values=["requester_id_v", "csat_v"],
        variableColumnName="field",
        valueColumnName="reading",
    )
)
