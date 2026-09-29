from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hired_on_running = Window.orderBy(F.col("hired_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_hired_on = Window.orderBy(F.col("hired_on").asc_nulls_last(), "id")
w_tier_by_hired_on = Window.partitionBy("tier").orderBy("hired_on", "id")
w_tier = Window.partitionBy("tier")
w_all_by_hired_on_ties = Window.orderBy("hired_on")
w_tier_seq_no_by_seq_no_runnin = Window.partitionBy("tier", "seq_no").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_agents = (
    df_support_agents
    .filter((F.col("hired_on").between(F.lit("2024-05-02").cast("date"), F.lit("2025-03-30").cast("date"))) & (F.col("tier").isNotNull()))
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
        F.substring(F.trim(F.col("agent_name")), 1, 2).alias("agent_name_prefix"),
    )
)

with_windows = (
    filtered_agents
    .select(
        "*",
        F.last("hired_on", ignorenulls=True).over(w_all_by_hired_on_running).alias("last_known_hired_on"),
        F.row_number().over(w_all_by_hired_on).alias("rn"),
        F.row_number().over(w_tier_by_hired_on).alias("seq_no"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.max("rn").over(w_tier).alias("tier_max_rn"),
        F.round(F.col("seq_no") - F.avg("seq_no").over(w_tier), 2).alias("seq_no_vs_tier_avg"),
        F.round(F.cume_dist().over(w_all_by_hired_on_ties), 3).alias("hired_on_cume_dist"),
        F.max("seq_no").over(w_tier_seq_no_by_seq_no_runnin).alias("max_seq_no_so_far"),
    )
)

result = (
    windowed
    .select(
        "id",
        "tier",
        "hired_on",
        "agent_name_prefix",
        "rn",
        "seq_no",
        "tier_max_rn",
        "seq_no_vs_tier_avg",
        "hired_on_cume_dist",
        "max_seq_no_so_far",
        F.when(F.col("tier").isin("T2", "T1"), "group_a").when(F.col("tier") == "T3", "group_b").otherwise("other").alias("tier_group"),
    )
    .orderBy(F.col("hired_on_cume_dist").desc(), "id")
    .limit(5)
)
