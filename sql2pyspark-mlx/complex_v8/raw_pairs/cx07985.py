from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_agent_ids_first_ope = Window.partitionBy("distinct_agent_ids", F.date_trunc("month", "first_opened_at")).orderBy("distinct_agent_ids", "priority").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_ids_distinct_agent = Window.partitionBy("distinct_ids", "distinct_agent_ids")
w_distinct_agent_ids_first_ope_2 = Window.partitionBy("distinct_agent_ids", F.date_trunc("month", "first_opened_at")).orderBy("distinct_agent_ids", "priority").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_n_rows_by_distinct = Window.partitionBy("priority", "n_rows").orderBy("distinct_ids", "priority")

recent = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
        F.when(F.col("requester_id") >= 22, "large").when(F.col("requester_id") >= 7, "medium").otherwise("small").alias("requester_id_tier"),
    )
)

with_activity = (
    recent.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.size(F.col("t.tags")) >= 1) & (F.col("t.requester_id") > 8), "left_anti")
    .filter(~F.array_contains(F.col("tags"), "upgrade"))
)

rollup_base = (
    with_activity
    .groupBy("priority")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.min("opened_at").alias("first_opened_at"),
    )
)

metrics = (
    rollup_base
    .select(
        "*",
        F.first("first_opened_at").over(w_distinct_agent_ids_first_ope).alias("first_first_opened_at"),
        F.size(F.collect_set("priority").over(w_distinct_ids_distinct_agent)).alias("distinct_priority_in_distinct_ids_distinct_agent"),
        F.max("n_rows").over(w_distinct_agent_ids_first_ope_2).alias("max_n_rows_so_far"),
        F.count(F.lit(1)).over(w_distinct_ids_distinct_agent).alias("rows_in_distinct_ids_distinct_agent_ids"),
        F.lead("n_rows", 2).over(w_priority_n_rows_by_distinct).alias("next2_n_rows"),
    )
)

result = (
    metrics
    .groupBy("distinct_agent_ids", "next2_n_rows")
    .pivot("priority", ["P4", "P1", "P3"])
    .agg(F.count("max_n_rows_so_far").alias("count_v"), F.max("max_n_rows_so_far").alias("max_v"))
    .orderBy("distinct_agent_ids", "next2_n_rows")
)
