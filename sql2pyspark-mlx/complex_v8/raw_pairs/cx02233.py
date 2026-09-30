from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment_by_risk_score_ties = Window.partitionBy("segment").orderBy("risk_score")
w_segment_region = Window.partitionBy("segment", "region")
w_all_by_risk_score_distance_r = Window.orderBy("risk_score_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_last_known_joined_on = Window.orderBy(F.col("last_known_joined_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_last_known_joined_on_2 = Window.orderBy(F.col("last_known_joined_on").asc_nulls_last(), "id")

filtered = (
    df_policyholders
    .filter((F.col("holder_name").isNotNull()) & (F.col("region").isin("South", "North", "West")))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.abs(F.col("risk_score") - 0.2).alias("risk_score_distance"),
    )
)

without_matches = (
    filtered.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left_semi")
    .filter(F.col("risk_score_distance") > 0.3)
)

with_windows = (
    without_matches
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        "risk_score_distance",
        F.dense_rank().over(w_segment_by_risk_score_ties).alias("risk_score_dense_rank"),
        F.min("risk_score_distance").over(w_segment_region).alias("segment_region_min_risk_score_distance"),
        F.last("joined_on", ignorenulls=True).over(w_all_by_risk_score_distance_r).alias("last_known_joined_on"),
    )
)

with_activity = (
    with_windows.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left_semi")
)

segment_stats = (
    with_activity
    .groupBy("segment")
    .agg(F.max("risk_score_dense_rank").alias("max_risk_score_dense_rank"))
)

compared = (
    with_activity.alias("s")
    .join(segment_stats.alias("g"), F.col("g.segment") == F.col("s.segment"), "inner")
    .filter(F.col("s.risk_score_dense_rank") == F.col("g.max_risk_score_dense_rank"))
    .select(
        "s.*",
        "g.max_risk_score_dense_rank",
        F.round(F.col("s.risk_score_dense_rank") - F.col("g.max_risk_score_dense_rank"), 2).alias("risk_score_dense_rank_minus_max"),
    )
)

annotated = (
    compared
    .select(
        "id",
        "holder_name",
        "region",
        "segment",
        "risk_score_dense_rank",
        "segment_region_min_risk_score_distance",
        "last_known_joined_on",
        "risk_score_dense_rank_minus_max",
        (F.floor(F.col("risk_score_dense_rank") / 2) * 2).alias("risk_score_dense_rank_bucket"),
    )
)

windowed = (
    annotated
    .select(
        "*",
        F.count(F.lit(1)).over(w_all_by_last_known_joined_on).alias("running_count"),
        (F.col("risk_score_dense_rank") - F.lag("risk_score_dense_rank").over(w_all_by_last_known_joined_on_2)).alias("risk_score_dense_rank_change"),
        F.count(F.lit(1)).over(w_segment_region).alias("rows_in_segment_region"),
    )
)

result = (
    windowed
    .select(
        "id",
        "risk_score_dense_rank_bucket",
        "risk_score_dense_rank_change",
        "running_count",
        "rows_in_segment_region",
    )
    .unpivot(
        ids=["id"],
        values=["risk_score_dense_rank_bucket", "risk_score_dense_rank_change", "running_count", "rows_in_segment_region"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
