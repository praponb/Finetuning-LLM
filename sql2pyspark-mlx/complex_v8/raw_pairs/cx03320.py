from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_label_is_explici = Window.partitionBy("is_explicit_label", "is_explicit").orderBy("greatest_distinct_ids_cnt")
w_greatest_distinct_ids_cnt_is = Window.partitionBy("greatest_distinct_ids_cnt", "is_explicit")
w_greatest_distinct_ids_cnt_by = Window.partitionBy("greatest_distinct_ids_cnt").orderBy(F.col("bpm_per_duration_sec_to_duration_sec_ratio").desc(), "is_explicit").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-03-29").cast("date")) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

rollup_base = (
    scoped
    .filter(F.col("artist_id") <= 3)
    .groupBy("is_explicit")
    .agg(
        F.count("*").alias("cnt"),
        F.max("release_date").alias("last_release_date"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("duration_sec").alias("total_duration_sec"),
        F.round(F.sum("bpm_per_duration_sec") / F.nullif(F.sum("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec_to_duration_sec_ratio"),
    )
    .filter(F.col("cnt") >= 2)
)

tagged = (
    rollup_base
    .select(
        "*",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.greatest(F.col("distinct_ids"), F.col("cnt")).alias("greatest_distinct_ids_cnt"),
        F.greatest(F.col("distinct_ids"), F.col("cnt")).alias("greatest_distinct_ids_cnt_2"),
    )
)

ranked = (
    tagged
    .select(
        "*",
        F.dense_rank().over(w_is_explicit_label_is_explici).alias("greatest_distinct_ids_cnt_dense_rank"),
        F.count(F.lit(1)).over(w_greatest_distinct_ids_cnt_is).alias("rows_in_greatest_distinct_ids_cnt_is_explicit"),
        F.sum("distinct_ids").over(w_greatest_distinct_ids_cnt_by).alias("running_distinct_ids"),
    )
)

enriched = (
    ranked
    .select(
        "*",
        F.last_day(F.col("last_release_date")).alias("last_release_date_month_end"),
        F.lower(F.regexp_replace(F.trim(F.col("is_explicit_label")), " ", "_")).alias("is_explicit_label_slug"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label_2"),
    )
)

enriched_2 = (
    enriched
    .select(
        "*",
        F.concat(F.col("is_explicit_label"), F.lit("#"), F.col("total_duration_sec").cast("string")).alias("is_explicit_label_ref"),
        F.when(F.col("running_distinct_ids") >= 75, "large").when(F.col("running_distinct_ids") >= 36, "medium").otherwise("small").alias("running_distinct_ids_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("is_explicit_label_slug")), " ", "_")).alias("is_explicit_label_slug_slug"),
    )
)

result = (
    enriched_2
    .select(
        "is_explicit",
        "cnt",
        "last_release_date",
        "distinct_ids",
        "total_duration_sec",
        "bpm_per_duration_sec_to_duration_sec_ratio",
        "is_explicit_label",
        "greatest_distinct_ids_cnt_2",
        "rows_in_greatest_distinct_ids_cnt_is_explicit",
        "running_distinct_ids",
        "last_release_date_month_end",
        "is_explicit_label_slug",
        "is_explicit_label_2",
        "is_explicit_label_ref",
        "running_distinct_ids_tier",
        "is_explicit_label_slug_slug",
        F.coalesce(F.col("running_distinct_ids"), F.lit(0)).alias("running_distinct_ids_filled"),
        F.coalesce(F.col("cnt"), F.lit(0)).alias("cnt_filled"),
    )
    .orderBy(
        F.col("is_explicit_label_slug").desc(),
        F.col("is_explicit_label_2").desc_nulls_first(),
    )
)
