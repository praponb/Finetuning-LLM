from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_joined_on_year = Window.partitionBy(F.year("joined_on"))
w_all_by_risk_score_distance_m = Window.orderBy(F.col("risk_score_distance").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_risk_score_distance_t = Window.orderBy(F.col("risk_score_distance").desc())
w_all_by_risk_score_distance_f = Window.orderBy(F.col("risk_score_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_policyholders
    .filter((F.col("region").isin("North", "South")) & (F.col("segment") == "RETAIL"))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.date_format(F.col("joined_on"), "MMM yyyy").alias("joined_on_label"),
        F.concat(F.col("segment"), F.lit("#"), F.col("id").cast("string")).alias("segment_ref"),
    )
)

annotated = (
    prepared
    .filter(F.col("risk_score") >= 0.3)
    .select(
        "id",
        "holder_name",
        "joined_on",
        "segment_ref",
        F.abs(F.col("risk_score") - 0.3).alias("risk_score_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("holder_name")), " ", "_")).alias("holder_name_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
    )
)

with_windows = (
    annotated
    .select(
        "id",
        "holder_name",
        "segment_ref",
        F.max("risk_score_distance").over(w_joined_on_year).alias("joined_on_year_max_risk_score_distance"),
        F.round(F.col("days_since_joined_on") / F.nullif(F.sum("days_since_joined_on").over(w_joined_on_year), F.lit(0)), 4).alias("days_since_joined_on_share_of_joined_on_year"),
        F.round(F.avg("days_since_joined_on").over(w_all_by_risk_score_distance_m), 2).alias("days_since_joined_on_avg_3"),
        F.round(F.percent_rank().over(w_all_by_risk_score_distance_t), 3).alias("risk_score_distance_percent_rank"),
        F.last("joined_on").over(w_all_by_risk_score_distance_f).alias("last_joined_on"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "holder_name",
        "segment_ref",
        "joined_on_year_max_risk_score_distance",
        "days_since_joined_on_share_of_joined_on_year",
        "last_joined_on",
    )
    .orderBy(F.col("days_since_joined_on_share_of_joined_on_year").desc())
)
