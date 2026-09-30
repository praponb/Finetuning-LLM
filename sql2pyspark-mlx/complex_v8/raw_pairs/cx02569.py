from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("region", "risk_score_tier").orderBy(F.col("joined_on").desc(), F.col("id").desc())

filtered_holders = (
    df_policyholders
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.substring(F.trim(F.col("holder_name")), 1, 2).alias("holder_name_prefix"),
        F.concat_ws(" / ", F.col("holder_name"), F.col("segment")).alias("holder_name_segment_label"),
        F.when(F.col("risk_score") >= 0.6, "large").when(F.col("risk_score") >= 0.2, "medium").otherwise("small").alias("risk_score_tier"),
    )
)

region_stats = (
    filtered_holders
    .groupBy("region")
    .agg(F.percentile_approx("risk_score", 0.5).alias("median_risk_score"))
)

above_average = (
    filtered_holders.alias("s")
    .join(region_stats.alias("g"), F.col("g.region") == F.col("s.region"), "inner")
    .filter(F.col("s.risk_score") >= F.col("g.median_risk_score"))
    .select(
        "s.*",
        "g.median_risk_score",
        F.round(F.col("s.risk_score") - F.col("g.median_risk_score"), 2).alias("risk_score_minus_median"),
    )
)

latest = (
    above_average
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .rollup("segment", "region")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count(F.when(F.length(F.trim(F.col("holder_name"))) >= 4, 1)).alias("n_length_trim_holder_name_4"),
        F.count("*").alias("row_count"),
        F.countDistinct("risk_score_tier").alias("distinct_risk_score_tiers"),
    )
)
