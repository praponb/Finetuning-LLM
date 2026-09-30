from pyspark.sql import functions as F

filtered_holders = (
    df_policyholders
    .filter((F.length(F.trim(F.col("holder_name"))) >= 6) & (F.year(F.col("joined_on")) == 2024))
    .select(
        "id",
        "risk_score",
        "joined_on",
        "segment",
        F.when(F.col("segment").isin("RETAIL", "VIP"), "group_a").when(F.col("segment") == "SME", "group_b").otherwise("other").alias("segment_group"),
    )
)

tagged = (
    filtered_holders
    .select(
        "id",
        "risk_score",
        "segment",
        "segment_group",
        F.date_add(F.col("joined_on"), 30).alias("joined_on_plus_30d"),
        F.date_format(F.col("joined_on"), "MMM yyyy").alias("joined_on_label"),
    )
)

result = (
    tagged
    .rollup("segment", "segment_group")
    .agg(
        F.count("*").alias("n_rows"),
        F.round(100 * F.count(F.when(F.col("segment") != "SME", 1)) / F.count("*"), 1).alias("pct_segment_sme"),
    )
    .orderBy(
        F.col("segment").asc_nulls_last(),
        F.col("segment_group").asc_nulls_last(),
    )
)
