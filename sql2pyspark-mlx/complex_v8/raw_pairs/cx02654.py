from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_segments = Window.partitionBy("distinct_segments")
w_distinct_segments_distinct_i = Window.partitionBy("distinct_segments", "distinct_ids").orderBy(F.col("n_segment_is_not_null").desc(), "segment", "joined_on_month")
w_all_by_avg_days_since_joined = Window.orderBy(F.col("avg_days_since_joined_on").desc_nulls_first(), "segment", "joined_on_month").rowsBetween(-3, -1)

cleaned_holders = (
    df_policyholders
    .filter(F.col("joined_on").isNotNull())
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.concat_ws(": ", F.col("region"), F.col("holder_name")).alias("region_holder_name_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
    )
)

per_group = (
    cleaned_holders
    .groupBy("segment", F.date_trunc("month", "joined_on").alias("joined_on_month"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("risk_score") / F.nullif(F.sum("days_since_joined_on"), F.lit(0)), 3).alias("risk_score_to_days_since_joined_on_ratio"),
        F.round(F.avg("days_since_joined_on"), 2).alias("avg_days_since_joined_on"),
        F.sum(F.when(F.col("segment").isNotNull(), 1).otherwise(0)).alias("n_segment_is_not_null"),
        F.countDistinct("segment").alias("distinct_segments"),
    )
)

ranked = (
    per_group
    .select(
        "*",
        F.sum("distinct_ids").over(w_distinct_segments).alias("distinct_segments_sum_distinct_ids"),
        F.row_number().over(w_distinct_segments_distinct_i).alias("seq_no"),
        F.round(F.avg("n_segment_is_not_null").over(w_distinct_segments), 2).alias("distinct_segments_avg_n_segment_is_not_null"),
        F.size(F.collect_set("segment").over(w_distinct_segments)).alias("distinct_segment_in_distinct_segments"),
        F.round(F.avg("distinct_ids").over(w_all_by_avg_days_since_joined), 2).alias("distinct_ids_avg_3_prior"),
    )
)

result = (
    ranked
    .select(
        "segment",
        "joined_on_month",
        "risk_score_to_days_since_joined_on_ratio",
        "distinct_segments_avg_n_segment_is_not_null",
        "avg_days_since_joined_on",
    )
    .unpivot(
        ids=["segment", "joined_on_month"],
        values=["risk_score_to_days_since_joined_on_ratio", "distinct_segments_avg_n_segment_is_not_null", "avg_days_since_joined_on"],
        variableColumnName="measure",
        valueColumnName="value",
    )
)
