from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("team").orderBy(F.col("hired_on").desc(), F.col("id").desc())

base = (
    df_support_agents
    .filter(F.col("tier") != "T1")
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
    )
)

qualified = (
    base.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.to_date(F.col("t.closed_at")) < F.lit("2025-02-26").cast("date")) & (F.col("t.channel").isin("web", "phone")), "left_anti")
)

deduped = (
    qualified
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

grouped = (
    deduped
    .groupBy(F.date_format("hired_on", "yyyy-MM").alias("hired_on_ym"))
    .agg(
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("agent_name"))).like("J%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_agent_name_like_j"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.upper(F.trim(F.col("agent_name"))).like("L%"), 1)).alias("n_upper_trim_agent_name_like_l"),
        F.min("hired_on").alias("first_hired_on"),
        F.count("*").alias("cnt"),
    )
)

result = (
    grouped
    .select(
        "hired_on_ym",
        F.col("n_upper_trim_agent_name_like_l").alias("n_upper_trim_agent_name_like_l_v"),
        F.col("distinct_ids").alias("distinct_ids_v"),
        F.col("cnt").alias("cnt_v"),
    )
    .unpivot(
        ids=["hired_on_ym"],
        values=["n_upper_trim_agent_name_like_l_v", "distinct_ids_v", "cnt_v"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .orderBy("hired_on_ym", "metric_name")
)
