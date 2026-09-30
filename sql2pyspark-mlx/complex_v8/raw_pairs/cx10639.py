from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment_or_unknown = Window.partitionBy("segment_or_unknown")
w_distinct_ids_region_by_disti = Window.partitionBy("distinct_ids", "region").orderBy(F.col("distinct_ids").desc())
w_distinct_ids_region_by_disti_2 = Window.partitionBy("distinct_ids", "region").orderBy(F.col("distinct_ids").desc(), "joined_on_ym", "region", "segment_or_unknown")

base_holders = (
    df_policyholders
    .filter(~F.col("region").isin("Central", "East"))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.coalesce(F.col("segment"), F.lit("UNKNOWN")).alias("segment_or_unknown"),
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
        F.when(F.col("risk_score") >= 0.6, "HIGH").when(F.col("risk_score") >= 0.3, "MEDIUM").otherwise("LOW").alias("risk_score_tier"),
    )
)

per_group = (
    base_holders
    .groupBy(
        F.date_format("joined_on", "yyyy-MM").alias("joined_on_ym"),
        "region",
        "segment_or_unknown",
    )
    .agg(
        F.countDistinct("segment").alias("distinct_segments"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("segment").isin("VIP", "SME"), 1).otherwise(0)).alias("n_segment_in_vip_sme"),
        F.round(F.stddev("risk_score"), 2).alias("stddev_risk_score"),
        F.sum("risk_score").alias("sum_risk_score_check"),
    )
    .filter(F.col("sum_risk_score_check") > 0.2)
    .drop("sum_risk_score_check")
)

tagged = (
    per_group
    .select(
        "joined_on_ym",
        "region",
        "segment_or_unknown",
        "distinct_segments",
        "distinct_ids",
        "stddev_risk_score",
        F.abs(F.col("stddev_risk_score") - 0.4).alias("stddev_risk_score_distance"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.round(F.col("distinct_ids") - F.avg("distinct_ids").over(w_segment_or_unknown), 2).alias("distinct_ids_vs_segment_or_unknown_avg"),
        F.round(F.col("stddev_risk_score_distance") - F.avg("stddev_risk_score_distance").over(w_segment_or_unknown), 2).alias("stddev_risk_score_distance_vs_segment_or_unknown"),
        F.round(F.percent_rank().over(w_distinct_ids_region_by_disti), 3).alias("distinct_ids_percent_rank"),
        F.round(F.percent_rank().over(w_distinct_ids_region_by_disti_2), 3).alias("distinct_ids_percent_rank_2"),
        F.ntile(3).over(w_distinct_ids_region_by_disti_2).alias("distinct_ids_tercile"),
        F.round(F.avg("stddev_risk_score_distance").over(w_segment_or_unknown), 2).alias("segment_or_unknown_avg_stddev_risk_score_distanc"),
    )
)

tagged_2 = (
    sequenced
    .select(
        "joined_on_ym",
        "region",
        "segment_or_unknown",
        "distinct_segments",
        "distinct_ids",
        "stddev_risk_score",
        "stddev_risk_score_distance",
        "distinct_ids_vs_segment_or_unknown_avg",
        "stddev_risk_score_distance_vs_segment_or_unknown",
        "distinct_ids_tercile",
        "segment_or_unknown_avg_stddev_risk_score_distanc",
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("region_or_unknown"),
    )
)

result = (
    tagged_2
    .select(
        "joined_on_ym",
        "region",
        "segment_or_unknown",
        "distinct_segments",
        "distinct_ids",
        "stddev_risk_score",
        "stddev_risk_score_distance",
        "distinct_ids_vs_segment_or_unknown_avg",
        "stddev_risk_score_distance_vs_segment_or_unknown",
        "distinct_ids_tercile",
        "segment_or_unknown_avg_stddev_risk_score_distanc",
        F.concat(F.col("segment_or_unknown"), F.lit("#"), F.col("distinct_segments").cast("string")).alias("segment_or_unknown_ref"),
    )
    .orderBy(F.col("region").desc())
)
