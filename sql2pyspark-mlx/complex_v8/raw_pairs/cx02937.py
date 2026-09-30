from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_closed_at_year = Window.partitionBy("agent_id", F.year("closed_at"))
w_closed_at_year_by_requester = Window.partitionBy(F.year("closed_at")).orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_priority_by_closed = Window.partitionBy("agent_id", "priority").orderBy("closed_at", "id")

base_tickets = (
    df_tickets
    .filter(F.col("csat") >= 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
    )
)

enriched = (
    base_tickets
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix_2"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.max("requester_id").over(w_agent_id_closed_at_year).alias("agent_id_closed_at_year_max_requester_id"),
        F.sum("requester_id").over(w_closed_at_year_by_requester).alias("running_requester_id"),
        F.lag("requester_id", 1, 0).over(w_agent_id_priority_by_closed).alias("prev_requester_id"),
    )
)

with_activity = (
    sequenced
    .crossJoin(sequenced.agg(F.max("requester_id").alias("overall_max_requester_id")))
    .filter(F.col("requester_id") < F.col("overall_max_requester_id"))
    .drop("overall_max_requester_id")
)

derived = (
    with_activity
    .select(
        "*",
        F.concat_ws(" / ", F.col("priority"), F.col("priority_slug")).alias("priority_priority_slug_label"),
        F.date_trunc("week", F.col("closed_at")).alias("closed_at_week"),
        F.coalesce(F.col("prev_requester_id"), F.lit(0)).alias("prev_requester_id_filled"),
    )
)

result = (
    derived
    .select(
        "id",
        "closed_at",
        "priority_slug",
        "priority_prefix",
        "priority_prefix_2",
        "prev_requester_id",
        "priority_priority_slug_label",
        "closed_at_week",
        "prev_requester_id_filled",
        F.when(F.col("priority").isin("P3", "P1"), "group_a").when(F.col("priority") == "P4", "group_b").otherwise("other").alias("priority_group"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
    .orderBy(F.col("prev_requester_id_filled").desc_nulls_first())
)
