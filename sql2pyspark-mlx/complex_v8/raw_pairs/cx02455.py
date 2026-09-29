from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_team_len = Window.partitionBy("cnt", "team_len")
w_team_len_cnt_by_distinct_tie = Window.partitionBy("team_len", "cnt").orderBy("distinct_tier_or_unknowns", "team_len").rowsBetween(-2, Window.currentRow)

filtered = (
    df_support_agents
    .filter(F.col("agent_name").isNotNull())
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.lower(F.regexp_replace(F.trim(F.col("tier")), " ", "_")).alias("tier_slug"),
        F.length(F.trim(F.col("team"))).alias("team_len"),
        F.substring(F.trim(F.col("team")), 1, 4).alias("team_prefix"),
    )
)

annotated = (
    filtered
    .select(
        "id",
        "agent_name",
        "team_len",
        "team_prefix",
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
    )
)

filtered_2 = (
    annotated.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.col("t.channel") == "chat"), "left_semi")
)

totals = (
    filtered_2
    .filter(F.upper(F.trim(F.col("agent_name"))).like("J%"))
    .groupBy("team_len")
    .agg(
        F.countDistinct("tier_or_unknown").alias("distinct_tier_or_unknowns"),
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.count("*").alias("cnt"),
    )
)

ranked = (
    totals
    .select(
        "*",
        F.round(F.avg("distinct_tier_or_unknowns").over(w_cnt_team_len), 2).alias("cnt_team_len_avg_distinct_tier_or_unknowns"),
        F.min("cnt").over(w_cnt_team_len).alias("cnt_team_len_min_cnt"),
        F.max("distinct_tier_or_unknowns").over(w_team_len_cnt_by_distinct_tie).alias("distinct_tier_or_unknowns_max_3"),
        F.round(F.col("distinct_tier_or_unknowns") / F.nullif(F.sum("distinct_tier_or_unknowns").over(w_cnt_team_len), F.lit(0)), 4).alias("distinct_tier_or_unknowns_share_of_cnt_team_len"),
        F.count(F.lit(1)).over(w_cnt_team_len).alias("rows_in_cnt_team_len"),
    )
)

labeled = (
    ranked
    .filter(F.col("rows_in_cnt_team_len") <= 11)
    .select(
        "*",
        (F.floor(F.col("cnt_team_len_min_cnt") / 25) * 25).alias("cnt_team_len_min_cnt_bucket"),
        F.when(F.col("cnt") >= 9, "A").when(F.col("cnt") >= 4, "B").otherwise("C").alias("cnt_tier"),
        F.size(F.col("agent_name_list")).alias("agent_name_list_count"),
    )
)

scored = (
    labeled
    .select(
        "team_len",
        "distinct_tier_or_unknowns",
        "agent_name_list",
        "cnt",
        "cnt_team_len_avg_distinct_tier_or_unknowns",
        "distinct_tier_or_unknowns_max_3",
        "cnt_team_len_min_cnt_bucket",
        "cnt_tier",
        "agent_name_list_count",
        F.lower(F.trim(F.col("cnt_tier"))).alias("cnt_tier_clean"),
    )
)

result = (
    scored
    .select(
        "team_len",
        F.col("distinct_tier_or_unknowns_max_3").alias("m_distinct_tier_or_unknowns_max_3"),
        F.col("agent_name_list_count").alias("m_agent_name_list_count"),
        F.col("cnt_team_len_min_cnt_bucket").alias("cnt_team_len_min_cnt_bucket_v"),
        F.col("distinct_tier_or_unknowns").alias("m_distinct_tier_or_unknowns"),
    )
    .unpivot(
        ids=["team_len"],
        values=["m_distinct_tier_or_unknowns_max_3", "m_agent_name_list_count", "cnt_team_len_min_cnt_bucket_v", "m_distinct_tier_or_unknowns"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
