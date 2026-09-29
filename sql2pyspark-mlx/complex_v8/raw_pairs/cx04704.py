from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region_by_stddev_risk_score = Window.partitionBy("region").orderBy(F.col("stddev_risk_score").asc_nulls_last(), "region").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_region_by_min_risk_score_tie = Window.partitionBy("region").orderBy("min_risk_score")

base_holders = (
    df_policyholders
    .filter((F.col("risk_score").between(0.1, 0.9)) | (F.year(F.col("joined_on")) == 2024))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "segment",
    )
)

stats = (
    base_holders
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("segment") != "SME", F.col("risk_score"))).alias("risk_score_segment_sme"),
        F.round(F.stddev("risk_score"), 2).alias("stddev_risk_score"),
        F.min("risk_score").alias("min_risk_score"),
        F.round(F.sum("risk_score"), 2).alias("total_risk_score"),
        F.sum(F.when(F.col("segment").isin("SME", "VIP"), F.col("risk_score")).otherwise(0)).alias("risk_score_segment_in_sme_vip"),
    )
)

sequenced = (
    stats
    .select(
        "region",
        "risk_score_segment_sme",
        "stddev_risk_score",
        "total_risk_score",
        F.nth_value("risk_score_segment_sme", 2).over(w_region_by_stddev_risk_score).alias("second_risk_score_segment_sme"),
        F.round(F.percent_rank().over(w_region_by_min_risk_score_tie), 3).alias("min_risk_score_percent_rank"),
    )
)

labeled = (
    sequenced
    .select(
        "*",
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("region_or_unknown"),
        F.when(F.col("region").isin("Central", "West"), "group_a").when(F.col("region") == "North", "group_b").otherwise("other").alias("region_group"),
    )
)

filtered = (
    labeled
    .crossJoin(labeled.agg(F.avg("stddev_risk_score").alias("overall_avg_stddev_risk_score")))
    .filter(F.col("stddev_risk_score") > F.col("overall_avg_stddev_risk_score"))
    .drop("overall_avg_stddev_risk_score")
)

result = (
    filtered
    .groupBy("region_or_unknown", "region_group")
    .pivot("region", ["Central", "North"])
    .agg(F.max("risk_score_segment_sme"))
    .orderBy("region_or_unknown", "region_group")
)
