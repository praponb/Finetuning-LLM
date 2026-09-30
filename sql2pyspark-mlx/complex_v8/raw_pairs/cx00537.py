from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("tier", "team").orderBy(F.col("agent_name_len").desc(), F.col("id").desc())

filtered_agents = (
    df_support_agents
    .filter(F.col("tier") == "T2")
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.last_day(F.col("hired_on")).alias("hired_on_month_end"),
    )
)

scored = (
    filtered_agents
    .filter(F.col("team").isin("Onboarding", "VIP"))
    .withColumn("agent_name_len", F.length(F.trim(F.col("agent_name"))))
)

qualified = (
    scored
    .crossJoin(scored.agg(F.avg("agent_name_len").alias("overall_avg_agent_name_len")))
    .filter(F.col("agent_name_len") > F.col("overall_avg_agent_name_len"))
    .drop("overall_avg_agent_name_len")
)

unique_rows = (
    qualified
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .groupingSets(
        [["agent_name_len", "team"], ["agent_name_len"], []],
        "agent_name_len", "team",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("agent_name_len", 0.5).alias("median_agent_name_len"),
    )
    .orderBy(
        F.col("agent_name_len").asc_nulls_last(),
        F.col("team").asc_nulls_last(),
    )
)
