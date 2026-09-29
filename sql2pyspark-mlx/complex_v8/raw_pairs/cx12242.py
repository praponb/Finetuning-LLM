from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_by_csat = Window.partitionBy("csat").orderBy("csat", "id", "tag_pos")
w_agent_id_csat_by_requester_i = Window.partitionBy("agent_id", "csat").orderBy(F.col("requester_id").desc(), "id", "tag_pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("tag", "agent_id").orderBy("csat_decile", "id", "tag_pos")

base = (
    df_tickets
    .filter(F.col("priority") == "P3")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
        (F.col("requester_id") - F.col("csat")).alias("requester_id_minus_csat"),
        F.least(F.col("csat"), F.col("requester_id")).alias("least_csat_requester_id"),
    )
)

flattened = (
    base
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "requester_id_minus_csat",
        "least_csat_requester_id",
        F.posexplode("tags").alias("tag_pos", "tag"),
    )
)

with_windows = (
    flattened
    .select(
        "id",
        "agent_id",
        "closed_at",
        "csat",
        "requester_id_minus_csat",
        "least_csat_requester_id",
        "tag_pos",
        "tag",
        F.ntile(10).over(w_csat_by_csat).alias("csat_decile"),
        F.count(F.lit(1)).over(w_agent_id_csat_by_requester_i).alias("running_count"),
    )
)

annotated = (
    with_windows
    .filter(F.col("running_count") < 18)
    .select(
        "id",
        "agent_id",
        "closed_at",
        "csat",
        "requester_id_minus_csat",
        "tag_pos",
        "tag",
        "csat_decile",
        "running_count",
        F.date_trunc("month", F.col("closed_at")).alias("closed_at_month"),
    )
)

latest_per_key = (
    annotated
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

tagged = (
    latest_per_key
    .withColumn("tag_slug", F.lower(F.regexp_replace(F.trim(F.col("tag")), " ", "_")))
)

agent_id_stats = (
    tagged
    .groupBy("agent_id")
    .agg(F.avg("running_count").alias("avg_running_count"))
)

vs_group = (
    tagged.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.running_count") > F.col("g.avg_running_count"))
    .select(
        "s.*",
        "g.avg_running_count",
        F.round(F.col("s.running_count") - F.col("g.avg_running_count"), 2).alias("running_count_minus_avg"),
    )
)

result = (
    vs_group
    .rollup("tag_pos", "csat_decile")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev("tag_pos"), 2).alias("stddev_tag_pos"),
        F.min("closed_at_month").alias("first_closed_at_month"),
    )
    .orderBy(
        F.col("tag_pos").asc_nulls_last(),
        F.col("csat_decile").asc_nulls_last(),
    )
)
