from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id").orderBy("requester_id_filled", "id")

prepared = (
    df_tickets
    .filter((F.col("requester_id") >= 4) & (F.col("priority").isin("P2", "P1", "P4")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
)

unique_rows = (
    prepared
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

channel_stats = (
    unique_rows
    .groupBy("channel")
    .agg(F.max("csat").alias("max_csat"))
)

compared = (
    unique_rows.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.csat") == F.col("g.max_csat"))
    .select(
        "s.*",
        "g.max_csat",
        F.round(F.col("s.csat") - F.col("g.max_csat"), 2).alias("csat_minus_max"),
    )
)

result = (
    compared
    .groupBy("agent_id", "csat")
    .pivot("channel", ["chat", "phone", "web", "email"])
    .agg(F.count("csat_minus_max").alias("count_v"), F.max("csat_minus_max").alias("max_v"))
)
