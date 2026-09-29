from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm_to_release_date_m = Window.orderBy(F.col("bpm_to_release_date_month_ratio").desc(), "is_explicit_label").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_label_distinct_i = Window.partitionBy("is_explicit_label", "distinct_is_explicit_labels")
w_all_by_bpm_to_release_date_m_2 = Window.orderBy(F.col("bpm_to_release_date_month_ratio").desc(), "is_explicit_label")
w_is_explicit_label_by_max_rel = Window.partitionBy("is_explicit_label").orderBy("max_release_date_month", "is_explicit_label")

scoped_tracks = (
    df_tracks
    .filter((F.col("release_date").isNotNull()) | (F.col("duration_sec").between(144, 409)))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.when(F.col("bpm") >= 142, "large").when(F.col("bpm") >= 94, "medium").otherwise("small").alias("bpm_tier"),
        F.month(F.col("release_date")).alias("release_date_month"),
    )
)

rollup_base = (
    scoped_tracks
    .groupBy("is_explicit_label")
    .agg(
        F.max("release_date_month").alias("max_release_date_month"),
        F.countDistinct("is_explicit_label").alias("distinct_is_explicit_labels"),
        F.sum("bpm").alias("total_bpm"),
        F.round(F.sum("bpm") / F.nullif(F.sum("release_date_month"), F.lit(0)), 3).alias("bpm_to_release_date_month_ratio"),
        F.sum("release_date_month").alias("total_release_date_month"),
        F.round(F.sum("duration_sec") / F.nullif(F.sum("release_date_month"), F.lit(0)), 3).alias("duration_sec_to_release_date_month_ratio"),
    )
)

sequenced = (
    rollup_base
    .select(
        "is_explicit_label",
        "distinct_is_explicit_labels",
        "total_bpm",
        "bpm_to_release_date_month_ratio",
        "total_release_date_month",
        F.nth_value("total_bpm", 2).over(w_all_by_bpm_to_release_date_m).alias("second_total_bpm"),
        F.count(F.lit(1)).over(w_is_explicit_label_distinct_i).alias("rows_in_is_explicit_label_distinct_is_explicit_l"),
        F.row_number().over(w_all_by_bpm_to_release_date_m_2).alias("seq_no"),
        F.round(F.col("total_bpm") - F.avg("total_bpm").over(w_is_explicit_label_distinct_i), 2).alias("total_bpm_vs_is_explicit_label_distinct_is_expli"),
        F.lead("total_release_date_month").over(w_is_explicit_label_by_max_rel).alias("next_total_release_date_month"),
    )
)

result = (
    sequenced
    .select(
        "is_explicit_label",
        "distinct_is_explicit_labels",
        "next_total_release_date_month",
    )
    .unpivot(
        ids=["is_explicit_label"],
        values=["distinct_is_explicit_labels", "next_total_release_date_month"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
