from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_distance_tier_csat_dist = Window.partitionBy("csat_distance_tier", "csat_distance")
w_csat_distance_tier_tags_coun = Window.partitionBy("csat_distance_tier", "tags_count").orderBy(F.col("tags_count").desc_nulls_first(), "id")
w_csat_distance_tier = Window.partitionBy("csat_distance_tier")
w_all_by_min_csat_distance_2_t = Window.orderBy(F.col("min_csat_distance_2").asc_nulls_last())
w_all_by_max_csat_distance_vs = Window.orderBy(F.col("max_csat_distance_vs_csat_distance_tier_avg").asc_nulls_last(), "csat_distance_tier", "csat_distance_2", "channel").rowsBetween(-1, -1)

recent = (
    df_tickets
    .filter(F.month(F.col("closed_at")).isin(1, 3))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
        F.abs(F.col("csat") - 4).alias("csat_distance"),
        F.size(F.col("tags")).alias("tags_count"),
    )
)

scored = (
    recent
    .filter(F.col("priority").isin("P3", "P1"))
    .select(
        "id",
        "requester_id",
        "channel",
        "tags",
        "csat_distance",
        "tags_count",
        F.abs(F.col("csat") - 3).alias("csat_distance_2"),
        F.initcap(F.trim(F.col("channel"))).alias("channel_clean"),
        F.when(F.col("csat_distance") >= 3, "large").when(F.col("csat_distance") >= 1, "medium").otherwise("small").alias("csat_distance_tier"),
    )
)

sequenced = (
    scored
    .select(
        "*",
        F.sum("requester_id").over(w_csat_distance_tier_csat_dist).alias("csat_distance_tier_csat_distance_sum_requester_i"),
        F.size(F.collect_set("channel").over(w_csat_distance_tier_csat_dist)).alias("distinct_channel_in_csat_distance_tier_csat_dist"),
        F.lead("tags_count", 2, 0).over(w_csat_distance_tier_tags_coun).alias("next2_tags_count"),
    )
)

stats = (
    sequenced
    .groupBy("csat_distance_tier", "csat_distance_2", "channel")
    .agg(
        F.min("csat_distance_2").alias("min_csat_distance_2"),
        F.max("csat_distance").alias("max_csat_distance"),
    )
)

ranked = (
    stats
    .select(
        "csat_distance_tier",
        "csat_distance_2",
        "channel",
        F.round(F.col("max_csat_distance") - F.avg("max_csat_distance").over(w_csat_distance_tier), 2).alias("max_csat_distance_vs_csat_distance_tier_avg"),
        F.dense_rank().over(w_all_by_min_csat_distance_2_t).alias("min_csat_distance_2_dense_rank"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.round(F.col("csat_distance_2") - F.avg("csat_distance_2").over(w_csat_distance_tier), 2).alias("csat_distance_2_vs_csat_distance_tier_avg"),
        F.round(F.avg("csat_distance_2").over(w_all_by_max_csat_distance_vs), 2).alias("csat_distance_2_avg_1_prior"),
    )
)

leaders = (
    with_windows
    .filter(F.col("min_csat_distance_2_dense_rank") <= 5)
    .drop("min_csat_distance_2_dense_rank")
)

result = (
    leaders
    .select(
        "csat_distance_tier",
        "csat_distance_2",
        "max_csat_distance_vs_csat_distance_tier_avg",
        "csat_distance_2_avg_1_prior",
        "csat_distance_2_vs_csat_distance_tier_avg",
    )
    .unpivot(
        ids=["csat_distance_tier", "csat_distance_2"],
        values=["max_csat_distance_vs_csat_distance_tier_avg", "csat_distance_2_avg_1_prior", "csat_distance_2_vs_csat_distance_tier_avg"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .orderBy("csat_distance_tier", "csat_distance_2", "metric_name")
)
