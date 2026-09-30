from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region_n_rows = Window.partitionBy("region", "n_rows")
w_region_n_segment_vip_by_min = Window.partitionBy("region", "n_segment_vip").orderBy("min_risk_score")
w_region_n_segment_vip_by_min_2 = Window.partitionBy("region", "n_segment_vip").orderBy("min_risk_score", "region").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_region_n_segment_vip_by_min_3 = Window.partitionBy("region", "n_segment_vip").orderBy("min_risk_score", "region").rowsBetween(-1, -1)
w_n_segment_vip_by_min_risk_sc = Window.partitionBy("n_segment_vip").orderBy("min_risk_score_adj_2")
w_second_n_rows_n_segment_vip = Window.partitionBy("second_n_rows", "n_segment_vip").orderBy("min_risk_score_dense_rank")
w_min_risk_score_adj_2_rank_mi = Window.partitionBy("min_risk_score_adj_2_rank", "min_risk_score_dense_rank")

cleaned = (
    df_policyholders
    .filter(F.col("risk_score") >= 0.2)
    .select(
        "id",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.round(F.col("risk_score") * 1.1, 2).alias("risk_score_adj"),
        F.round(F.col("risk_score") * 0.25, 2).alias("risk_score_adj_2"),
    )
)

stats = (
    cleaned
    .groupBy("region")
    .agg(
        F.count("*").alias("n_rows"),
        F.min("risk_score").alias("min_risk_score"),
        F.min("risk_score_adj_2").alias("min_risk_score_adj_2"),
        F.count(F.when(F.col("segment") == "VIP", 1)).alias("n_segment_vip"),
        F.round(F.avg("risk_score_adj"), 2).alias("avg_risk_score_adj"),
    )
)

scored = (
    stats
    .select(
        "*",
        F.concat(F.col("region"), F.lit("#"), F.col("n_rows").cast("string")).alias("region_ref"),
        F.concat(F.col("region"), F.lit("#"), F.col("n_rows").cast("string")).alias("region_ref_2"),
        F.concat(F.col("region"), F.lit("#"), F.col("n_segment_vip").cast("string")).alias("region_ref_3"),
    )
)

windowed = (
    scored
    .select(
        "*",
        F.round(F.col("min_risk_score_adj_2") - F.avg("min_risk_score_adj_2").over(w_region_n_rows), 2).alias("min_risk_score_adj_2_vs_region_n_rows_avg"),
        F.dense_rank().over(w_region_n_segment_vip_by_min).alias("min_risk_score_dense_rank"),
        F.nth_value("n_rows", 2).over(w_region_n_segment_vip_by_min_2).alias("second_n_rows"),
        F.sum("min_risk_score_adj_2").over(w_region_n_segment_vip_by_min_3).alias("min_risk_score_adj_2_sum_1_prior"),
        F.rank().over(w_n_segment_vip_by_min_risk_sc).alias("min_risk_score_adj_2_rank"),
    )
)

ranked = (
    windowed
    .select(
        "region",
        "n_rows",
        "min_risk_score",
        "n_segment_vip",
        "avg_risk_score_adj",
        "region_ref",
        "region_ref_3",
        "min_risk_score_dense_rank",
        "second_n_rows",
        "min_risk_score_adj_2_sum_1_prior",
        "min_risk_score_adj_2_rank",
        F.dense_rank().over(w_second_n_rows_n_segment_vip).alias("min_risk_score_dense_rank_dense_rank"),
        F.round(F.col("min_risk_score_adj_2") - F.avg("min_risk_score_adj_2").over(w_min_risk_score_adj_2_rank_mi), 2).alias("min_risk_score_adj_2_vs_min_risk_score_adj_2_ran"),
    )
)

scored_2 = (
    ranked
    .select(
        "region",
        "n_segment_vip",
        "avg_risk_score_adj",
        "region_ref",
        "region_ref_3",
        "min_risk_score_dense_rank",
        "min_risk_score_adj_2_rank",
        "min_risk_score_adj_2_vs_min_risk_score_adj_2_ran",
        F.when(F.col("n_segment_vip") >= 8, "large").when(F.col("n_segment_vip") >= 4, "medium").otherwise("small").alias("n_segment_vip_tier"),
        F.round(F.col("min_risk_score") * 0.9, 2).alias("min_risk_score_adj"),
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("region_or_unknown"),
    )
)

result = (
    scored_2
    .select(
        "region",
        "n_segment_vip",
        "avg_risk_score_adj",
        "region_ref",
        "region_ref_3",
        "min_risk_score_adj_2_rank",
        "min_risk_score_adj_2_vs_min_risk_score_adj_2_ran",
        "n_segment_vip_tier",
        "region_or_unknown",
    )
    .orderBy(F.col("min_risk_score_adj_2_vs_min_risk_score_adj_2_ran").desc())
)
