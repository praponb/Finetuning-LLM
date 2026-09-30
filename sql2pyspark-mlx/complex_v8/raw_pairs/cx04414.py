from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment = Window.partitionBy("segment")
w_region_segment_by_risk_score = Window.partitionBy("region", "segment").orderBy("risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_policyholders
    .filter(F.col("risk_score") > 0.2)
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.date_trunc("week", F.col("joined_on")).alias("joined_on_week"),
    )
)

windowed = (
    prepared
    .select(
        "id",
        "risk_score",
        "joined_on",
        "segment",
        "joined_on_week",
        F.size(F.collect_set("region").over(w_segment)).alias("distinct_region_in_segment"),
        F.nth_value("joined_on", 2).over(w_region_segment_by_risk_score).alias("second_joined_on"),
    )
)

enriched = (
    windowed
    .filter(F.col("risk_score") >= 0.2)
    .select(
        "*",
        F.date_trunc("week", F.col("second_joined_on")).alias("second_joined_on_week"),
        F.length(F.trim(F.col("segment"))).alias("segment_len"),
        F.when(F.col("risk_score") >= 0.6, "HIGH").when(F.col("risk_score") >= 0.3, "MEDIUM").otherwise("LOW").alias("risk_score_tier"),
    )
)

per_group = (
    enriched
    .groupBy(
        F.date_format("second_joined_on_week", "yyyy-MM").alias("second_joined_on_week_ym"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("joined_on").alias("last_joined_on"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("segment").alias("distinct_segments"),
    )
)

result = (
    per_group
    .filter(F.col("distinct_ids") < 8)
    .select(
        "second_joined_on_week_ym",
        "cnt",
        "last_joined_on",
        "distinct_ids",
        "distinct_segments",
    )
)
