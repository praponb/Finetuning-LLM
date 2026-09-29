from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_all_by_distinct_ids = Window.orderBy(F.col("distinct_ids").asc_nulls_last(), "priority")
w_distinct_ids_cnt_by_min_requ = Window.partitionBy("distinct_ids", "cnt").orderBy(F.col("min_requester_id_plus_csat").desc(), "priority").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_cnt = Window.partitionBy("cnt")

base = (
    df_tickets
    .filter(F.col("priority") == "P4")
    .select(
        "id",
        "agent_id",
        "priority",
        "csat",
        "tags",
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

summary = (
    base
    .groupBy("priority")
    .agg(
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.count("*").alias("cnt"),
        F.max("requester_id_plus_csat").alias("max_requester_id_plus_csat"),
        F.min("requester_id_plus_csat").alias("min_requester_id_plus_csat"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("cnt") >= 3)
)

windowed = (
    summary
    .select(
        "*",
        F.round(F.col("distinct_ids") - F.avg("distinct_ids").over(w_priority), 2).alias("distinct_ids_vs_priority_avg"),
        F.min("cnt").over(w_priority).alias("priority_min_cnt"),
    )
)

labeled = (
    windowed
    .withColumn("distinct_agent_ids_per_priority_min_cnt", F.round(F.col("distinct_agent_ids") / F.nullif(F.col("priority_min_cnt"), F.lit(0)), 3))
)

with_windows = (
    labeled
    .select(
        "*",
        (F.col("distinct_agent_ids") - F.lag("distinct_agent_ids").over(w_all_by_distinct_ids)).alias("distinct_agent_ids_change"),
        F.last("distinct_agent_ids", ignorenulls=True).over(w_distinct_ids_cnt_by_min_requ).alias("last_known_distinct_agent_ids"),
        F.first("cnt", ignorenulls=True).over(w_distinct_ids_cnt_by_min_requ).alias("first_known_cnt"),
        F.round(F.col("priority_min_cnt") - F.avg("priority_min_cnt").over(w_cnt), 2).alias("priority_min_cnt_vs_cnt_avg"),
    )
)

result = (
    with_windows
    .select(
        "priority",
        "cnt",
        "max_requester_id_plus_csat",
        "min_requester_id_plus_csat",
        "distinct_ids_vs_priority_avg",
        "priority_min_cnt",
        "distinct_agent_ids_per_priority_min_cnt",
        "distinct_agent_ids_change",
        (F.col("cnt") - F.col("first_known_cnt")).alias("cnt_minus_first_known_cnt"),
    )
    .orderBy(F.col("distinct_agent_ids_per_priority_min_cnt").asc_nulls_last())
)
