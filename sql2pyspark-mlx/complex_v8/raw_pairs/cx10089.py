from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("priority", "channel").orderBy(F.col("csat").desc(), F.col("id").desc())

cleaned = (
    df_tickets
    .filter((F.col("channel").isin("phone", "chat", "web")) & (F.col("priority") == "P1"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.upper(F.trim(F.col("channel"))).alias("channel_clean"),
    )
)

unique_rows = (
    cleaned
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

exploded = (
    unique_rows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "channel_clean",
        F.explode("tags").alias("tag"),
    )
)

result = (
    exploded
    .rollup("priority", "csat", "channel")
    .agg(
        F.grouping("priority").alias("is_total_priority"),
        F.count(F.when(F.col("channel").isin("web", "phone", "chat"), 1)).alias("n_channel_in_web_phone_chat"),
        F.min("csat").alias("min_csat"),
    )
    .select(
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "csat",
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        "is_total_priority",
        "n_channel_in_web_phone_chat",
        "min_csat",
    )
    .orderBy(
        F.col("priority").asc_nulls_last(),
        F.col("csat").asc_nulls_last(),
        F.col("channel").asc_nulls_last(),
    )
)
