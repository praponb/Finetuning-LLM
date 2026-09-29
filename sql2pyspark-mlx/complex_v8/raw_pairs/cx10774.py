from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_ids = Window.partitionBy("distinct_ids")
w_bpm_tier = Window.partitionBy("bpm_tier")
w_distinct_ids_by_bpm_is_expli = Window.partitionBy("distinct_ids").orderBy(F.col("bpm_is_explicit_false").desc(), "bpm_tier").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_ids_bpm_tier_by_dis = Window.partitionBy("distinct_ids", "bpm_tier").orderBy("distinct_ids", "bpm_tier")

filtered_tracks = (
    df_tracks
    .filter(F.col("title").isNotNull())
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.size(F.col("moods")).alias("moods_count"),
    )
)

rollup_base = (
    filtered_tracks
    .groupBy(
        F.when(F.col("bpm") >= 147, "large").when(F.col("bpm") >= 100, "medium").otherwise("small").alias("bpm_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("bpm", 0.5).alias("median_bpm"),
        F.max("duration_sec").alias("max_duration_sec"),
        F.sum(F.when(~F.col("is_explicit"), F.col("bpm"))).alias("bpm_is_explicit_false"),
        F.avg("moods_count").alias("avg_moods_count_check"),
    )
    .filter(F.col("avg_moods_count_check") > 1)
    .drop("avg_moods_count_check")
)

with_trends = (
    rollup_base
    .select(
        "bpm_tier",
        "distinct_ids",
        "max_duration_sec",
        "bpm_is_explicit_false",
        F.min("bpm_is_explicit_false").over(w_distinct_ids).alias("distinct_ids_min_bpm_is_explicit_false"),
        F.round(F.col("median_bpm") - F.avg("median_bpm").over(w_distinct_ids), 2).alias("median_bpm_vs_distinct_ids_avg"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "*",
        F.round(F.col("distinct_ids") / F.nullif(F.sum("distinct_ids").over(w_bpm_tier), F.lit(0)), 4).alias("distinct_ids_share_of_bpm_tier"),
        F.last("max_duration_sec", ignorenulls=True).over(w_distinct_ids_by_bpm_is_expli).alias("last_known_max_duration_sec"),
        F.sum("bpm_is_explicit_false").over(w_bpm_tier).alias("bpm_tier_sum_bpm_is_explicit_false"),
        F.count(F.lit(1)).over(w_bpm_tier).alias("rows_in_bpm_tier"),
        (F.col("bpm_is_explicit_false") - F.lag("bpm_is_explicit_false").over(w_distinct_ids_bpm_tier_by_dis)).alias("bpm_is_explicit_false_change"),
    )
)

result = (
    with_trends_2
    .select(
        "bpm_tier",
        "distinct_ids_share_of_bpm_tier",
        "median_bpm_vs_distinct_ids_avg",
    )
    .unpivot(
        ids=["bpm_tier"],
        values=["distinct_ids_share_of_bpm_tier", "median_bpm_vs_distinct_ids_avg"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("bpm_tier", "field")
)
