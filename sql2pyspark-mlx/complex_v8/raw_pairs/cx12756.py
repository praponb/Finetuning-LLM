from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_joined_on_month_region_by_jo = Window.partitionBy(F.date_trunc("month", "joined_on"), "region").orderBy(F.col("joined_on").desc(), "id")
w_region_segment = Window.partitionBy("region", "segment")
w_segment_by_risk_score_distan = Window.partitionBy("segment").orderBy("risk_score_distance_vs_region_segment_avg", "id").rowsBetween(-2, -1)
w_joined_on_month = Window.partitionBy(F.month("joined_on"))
w_segment_by_risk_score_distan_2 = Window.partitionBy("segment").orderBy("risk_score_distance_vs_region_segment_avg")

cleaned = (
    df_policyholders
    .filter((F.col("joined_on").isNotNull()) & (F.col("region").isin("East", "South")))
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
        F.abs(F.col("risk_score") - 0.6).alias("risk_score_distance"),
    )
)

sequenced = (
    cleaned
    .select(
        "id",
        "holder_name",
        "joined_on",
        "segment",
        "risk_score_distance",
        F.lead("risk_score_distance").over(w_joined_on_month_region_by_jo).alias("next_risk_score_distance"),
        F.round(F.col("risk_score_distance") - F.avg("risk_score_distance").over(w_region_segment), 2).alias("risk_score_distance_vs_region_segment_avg"),
    )
)

segment_stats = (
    sequenced
    .groupBy("segment")
    .agg(F.avg("risk_score_distance").alias("avg_risk_score_distance"))
)

above_average = (
    sequenced.alias("s")
    .join(segment_stats.alias("g"), F.col("g.segment") == F.col("s.segment"), "inner")
    .filter(F.col("s.risk_score_distance") > F.col("g.avg_risk_score_distance"))
    .select(
        "s.*",
        "g.avg_risk_score_distance",
        F.round(F.col("s.risk_score_distance") - F.col("g.avg_risk_score_distance"), 2).alias("risk_score_distance_minus_avg"),
    )
)

qualified = (
    above_average.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.end_date").between(F.lit("2025-11-20").cast("date"), F.lit("2026-09-23").cast("date"))) & (F.col("p.premium") > 2111.3), "left_semi")
    .filter(F.col("segment").isNotNull())
)

ranked = (
    qualified
    .select(
        "*",
        F.round(F.avg("avg_risk_score_distance").over(w_segment_by_risk_score_distan), 2).alias("avg_risk_score_distance_avg_2_prior"),
        F.round(F.col("avg_risk_score_distance") - F.avg("avg_risk_score_distance").over(w_joined_on_month), 2).alias("avg_risk_score_distance_vs_joined_on_month_avg"),
        F.size(F.collect_set("segment").over(w_joined_on_month)).alias("distinct_segment_in_joined_on_month"),
        F.rank().over(w_segment_by_risk_score_distan_2).alias("risk_score_distance_vs_region_segment_avg_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "risk_score_distance",
        "next_risk_score_distance",
        "risk_score_distance_vs_region_segment_avg",
        "avg_risk_score_distance",
        "risk_score_distance_minus_avg",
        "avg_risk_score_distance_avg_2_prior",
        "avg_risk_score_distance_vs_joined_on_month_avg",
        "distinct_segment_in_joined_on_month",
        "risk_score_distance_vs_region_segment_avg_rank",
        F.substring(F.trim(F.col("holder_name")), 1, 2).alias("holder_name_prefix"),
    )
    .orderBy(
        F.col("risk_score_distance").desc(),
        F.col("risk_score_distance_vs_region_segment_avg").desc(),
    )
)
