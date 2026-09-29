from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region_segment_by_risk_score = Window.partitionBy("region", "segment").orderBy(F.col("risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_region_segment_by_risk_score_2 = Window.partitionBy("region", "segment").orderBy(F.col("risk_score").desc())
w_joined_on_year_segment = Window.partitionBy(F.year("joined_on"), "segment")
w_all_by_risk_score_share_of_j = Window.orderBy("risk_score_share_of_joined_on_year_segment", "id")
w_segment = Window.partitionBy("segment")

filtered = (
    df_policyholders
    .filter((F.col("joined_on") >= F.lit("2024-06-29").cast("date")) & (F.col("region").isNotNull()))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.date_format(F.col("joined_on"), "yyyy-MM-dd").alias("joined_on_label"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "region",
        "risk_score",
        "segment",
        F.first("risk_score", ignorenulls=True).over(w_region_segment_by_risk_score).alias("first_known_risk_score"),
        F.round(F.percent_rank().over(w_region_segment_by_risk_score_2), 3).alias("risk_score_percent_rank"),
        F.sum("risk_score").over(w_joined_on_year_segment).alias("joined_on_year_segment_sum_risk_score"),
        F.round(F.col("risk_score") / F.nullif(F.sum("risk_score").over(w_joined_on_year_segment), F.lit(0)), 4).alias("risk_score_share_of_joined_on_year_segment"),
        F.first("joined_on", ignorenulls=True).over(w_region_segment_by_risk_score).alias("first_known_joined_on"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        (F.col("first_known_risk_score") - F.lag("first_known_risk_score").over(w_all_by_risk_score_share_of_j)).alias("first_known_risk_score_change"),
        F.round(F.col("first_known_risk_score") - F.avg("first_known_risk_score").over(w_segment), 2).alias("first_known_risk_score_vs_segment_avg"),
        F.max("first_known_risk_score").over(w_segment).alias("segment_max_first_known_risk_score"),
        F.size(F.collect_set("region").over(w_segment)).alias("distinct_region_in_segment"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "region",
        "risk_score",
        "segment",
        "risk_score_percent_rank",
        "joined_on_year_segment_sum_risk_score",
        "risk_score_share_of_joined_on_year_segment",
        "first_known_joined_on",
        "first_known_risk_score_change",
        "first_known_risk_score_vs_segment_avg",
        "segment_max_first_known_risk_score",
        F.abs(F.col("risk_score_percent_rank") - 0.3).alias("risk_score_percent_rank_distance"),
    )
    .orderBy(
        F.col("first_known_risk_score_vs_segment_avg").asc(),
        F.col("segment").asc(),
    )
)
