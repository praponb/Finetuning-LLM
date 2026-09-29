from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_joined_on_prior_3 = Window.orderBy("joined_on", "id").rowsBetween(-3, -1)
w_region_region_or_unknown_by = Window.partitionBy("region", "region_or_unknown").orderBy("joined_on", "id")
w_region_or_unknown_region = Window.partitionBy("region_or_unknown", "region")
w_all_by_joined_on_ties = Window.orderBy("joined_on")
w_region_region_or_unknown_by_2 = Window.partitionBy("region", "region_or_unknown").orderBy("joined_on")
w_region_or_unknown_region_by = Window.partitionBy("region_or_unknown", "region").orderBy(F.col("risk_score_distance").desc_nulls_first(), "id")
w_region_or_unknown_region_by_2 = Window.partitionBy("region_or_unknown", "region").orderBy(F.col("risk_score_distance").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_region_or_unknown_region_by_3 = Window.partitionBy("region_or_unknown", "region").orderBy(F.col("risk_score_distance").desc_nulls_first())

prepared = (
    df_policyholders
    .filter((F.col("segment") == "VIP") & (F.col("region").isin("East", "Central", "West")))
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("region_or_unknown"),
        F.abs(F.col("risk_score") - 0.3).alias("risk_score_distance"),
    )
)

metrics = (
    prepared
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "region_or_unknown",
        "risk_score_distance",
        F.sum("risk_score_distance").over(w_all_by_joined_on_prior_3).alias("risk_score_distance_sum_3_prior"),
        F.lag("joined_on").over(w_region_region_or_unknown_by).alias("prev_joined_on"),
        F.min("risk_score_distance").over(w_region_or_unknown_region).alias("region_or_unknown_region_min_risk_score_distance"),
        F.round(F.percent_rank().over(w_all_by_joined_on_ties), 3).alias("joined_on_percent_rank"),
        F.round(F.cume_dist().over(w_region_region_or_unknown_by_2), 3).alias("joined_on_cume_dist"),
        F.row_number().over(w_region_or_unknown_region_by).alias("risk_score_distance_seq"),
        F.first("joined_on", ignorenulls=True).over(w_region_or_unknown_region_by_2).alias("first_known_joined_on"),
        F.rank().over(w_region_region_or_unknown_by).alias("joined_on_rank"),
        F.rank().over(w_region_or_unknown_region_by_3).alias("risk_score_distance_rank"),
        F.dense_rank().over(w_region_or_unknown_region_by_3).alias("risk_score_distance_dense_rank"),
        F.round(F.col("risk_score_distance") / F.nullif(F.sum("risk_score_distance").over(w_region_or_unknown_region), F.lit(0)), 4).alias("risk_score_distance_share_of_region_or_unknown_r"),
        F.ntile(5).over(w_region_region_or_unknown_by).alias("joined_on_quintile"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.abs(F.col("risk_score_distance_rank") - 5).alias("risk_score_distance_rank_distance"),
        F.date_format(F.col("first_known_joined_on"), "yyyy-MM-dd").alias("first_known_joined_on_label"),
    )
)

result = (
    enriched
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "region_or_unknown",
        "risk_score_distance",
        "risk_score_distance_sum_3_prior",
        "prev_joined_on",
        "joined_on_cume_dist",
        "first_known_joined_on",
        "joined_on_rank",
        "risk_score_distance_dense_rank",
        "risk_score_distance_share_of_region_or_unknown_r",
        "joined_on_quintile",
        "first_known_joined_on_label",
    )
    .orderBy("joined_on", "id")
    .limit(20)
)
