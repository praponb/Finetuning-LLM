from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("priority_group").orderBy(F.col("requester_id_minus_csat").desc(), F.col("id").desc())
w_dedup = Window.partitionBy("agent_id", "priority_group").orderBy(F.col("closed_at").desc(), F.col("id").desc())

filtered = (
    df_tickets
    .filter(F.col("csat") > 1)
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        (F.col("requester_id") - F.col("csat")).alias("requester_id_minus_csat"),
        F.when(F.col("priority").isin("P3", "P4"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

without_matches = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel") != "phone") & (F.col("t.csat") <= 5), "left_semi")
)

unique_rows = (
    without_matches
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

priority_group_stats = (
    unique_rows
    .groupBy("priority_group")
    .agg(F.max("csat").alias("max_csat"))
)

vs_group = (
    unique_rows.alias("s")
    .join(priority_group_stats.alias("g"), F.col("g.priority_group") == F.col("s.priority_group"), "inner")
    .filter(F.col("s.csat") == F.col("g.max_csat"))
    .select(
        "s.*",
        "g.max_csat",
        F.round(F.col("s.csat") - F.col("g.max_csat"), 2).alias("csat_minus_max"),
    )
)

annotated = (
    vs_group
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

latest = (
    annotated
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

scored = (
    latest
    .select(
        "*",
        F.date_trunc("quarter", F.col("closed_at")).alias("closed_at_quarter"),
        F.greatest(F.col("days_since_closed_at"), F.col("requester_id_minus_csat")).alias("greatest_days_since_closed_at_requester_id_minus"),
        F.coalesce(F.col("requester_id_minus_csat"), F.lit(0)).alias("requester_id_minus_csat_filled"),
    )
)

result = (
    scored
    .filter(F.length(F.trim(F.col("priority_clean"))) >= 8)
    .select(
        "id",
        "closed_at",
        "priority",
        "csat",
        "requester_id_minus_csat",
        "priority_group",
        "days_since_opened_at",
        "max_csat",
        "csat_minus_max",
        "days_since_closed_at",
        "priority_clean",
        "closed_at_quarter",
        "greatest_days_since_closed_at_requester_id_minus",
        F.when(F.col("priority").isin("P4", "P2"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group_2"),
    )
    .orderBy("greatest_days_since_closed_at_requester_id_minus")
)
