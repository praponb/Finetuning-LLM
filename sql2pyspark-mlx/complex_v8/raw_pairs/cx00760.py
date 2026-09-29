from pyspark.sql import functions as F

recent_holders = (
    df_policyholders
    .filter(F.col("segment").isin("RETAIL", "SME"))
    .select(
        "id",
        "risk_score",
        "joined_on",
        "segment",
        F.when(F.col("risk_score") >= 0.6, "large").when(F.col("risk_score") >= 0.3, "medium").otherwise("small").alias("risk_score_tier"),
        F.concat_ws(": ", F.col("region"), F.col("segment")).alias("region_segment_label"),
    )
)

annotated = (
    recent_holders
    .filter(F.col("risk_score") < 0.8)
    .select(
        "id",
        "joined_on",
        "risk_score_tier",
        F.date_format(F.col("joined_on"), "MMM yyyy").alias("joined_on_label"),
        F.substring(F.trim(F.col("region_segment_label")), 1, 3).alias("region_segment_label_prefix"),
    )
)

totals = (
    annotated
    .groupBy(F.year("joined_on").alias("joined_on_year"))
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("joined_on").alias("first_joined_on"),
        F.max("joined_on").alias("last_joined_on"),
    )
)

result = (
    totals
    .select(
        "joined_on_year",
        F.col("distinct_ids").alias("distinct_ids_v"),
        F.col("row_count").alias("row_count_v"),
    )
    .unpivot(
        ids=["joined_on_year"],
        values=["distinct_ids_v", "row_count_v"],
        variableColumnName="field",
        valueColumnName="reading",
    )
)
