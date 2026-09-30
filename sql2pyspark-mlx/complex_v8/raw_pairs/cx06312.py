from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region_segment_by_risk_score = Window.partitionBy("region", "segment").orderBy("risk_score_filled")
w_region_segment_by_risk_score_2 = Window.partitionBy("region", "segment").orderBy("risk_score_filled", "id").rowsBetween(-1, -1)

recent = (
    df_policyholders
    .filter(F.col("joined_on") < F.lit("2025-07-02").cast("date"))
    .select(
        "id",
        "holder_name",
        "region",
        "segment",
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
    )
)

with_windows = (
    recent
    .select(
        "id",
        "holder_name",
        "segment",
        "risk_score_filled",
        F.dense_rank().over(w_region_segment_by_risk_score).alias("risk_score_filled_dense_rank"),
        F.sum("risk_score_filled").over(w_region_segment_by_risk_score_2).alias("risk_score_filled_sum_1_prior"),
    )
)

tagged = (
    with_windows
    .select(
        "*",
        F.concat_ws("|", F.col("segment"), F.col("holder_name")).alias("segment_holder_name_label"),
        F.concat(F.col("segment"), F.lit("#"), F.col("id").cast("string")).alias("segment_ref"),
        F.coalesce(F.col("risk_score_filled"), F.lit(0.0)).alias("risk_score_filled_filled"),
    )
)

result = (
    tagged
    .groupingSets(
        [["segment", "risk_score_filled_dense_rank"], ["segment"], ["risk_score_filled_dense_rank"], []],
        "segment", "risk_score_filled_dense_rank",
    )
    .agg(
        F.grouping("segment").alias("is_total_segment"),
        F.count("*").alias("row_count"),
    )
    .select(
        F.coalesce(F.col("segment"), F.lit("ALL")).alias("segment"),
        "risk_score_filled_dense_rank",
        "is_total_segment",
        "row_count",
    )
    .orderBy(
        F.col("segment").asc_nulls_last(),
        F.col("risk_score_filled_dense_rank").asc_nulls_last(),
    )
)
