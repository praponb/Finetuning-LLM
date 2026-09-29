from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_n_rows = Window.orderBy(F.col("n_rows").desc(), "title_len")
w_title_len_tier = Window.partitionBy("title_len_tier")
w_rn_n_rows = Window.partitionBy("rn", "n_rows")
w_rn_title_len_by_n_rows_runni = Window.partitionBy("rn", "title_len").orderBy(F.col("n_rows").desc(), "title_len").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_rows_by_rows_in_title_len = Window.partitionBy("n_rows").orderBy("rows_in_title_len_tier", "title_len")
w_title_len_by_rows_in_title_l = Window.partitionBy("title_len").orderBy(F.col("rows_in_title_len_tier").desc())
w_rn_title_len_by_n_rows = Window.partitionBy("rn", "title_len").orderBy(F.col("n_rows").desc(), "title_len")

scoped_tracks = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "chill")) & (F.col("bpm") > 89))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

per_group = (
    scoped_tracks
    .groupBy("title_len")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.upper(F.trim(F.col("title"))).like("G%"), F.col("title_len"))).alias("title_len_upper_trim_title_like_g"),
    )
    .filter(F.col("n_rows") >= 2)
)

filtered = (
    per_group
    .crossJoin(per_group.agg(F.min("title_len_upper_trim_title_like_g").alias("overall_min_title_len_upper_trim_title_like_g")))
    .filter(F.col("title_len_upper_trim_title_like_g") > F.col("overall_min_title_len_upper_trim_title_like_g"))
    .drop("overall_min_title_len_upper_trim_title_like_g")
)

annotated = (
    filtered
    .select(
        "*",
        F.round(F.col("distinct_ids") / F.nullif(F.col("title_len"), F.lit(0)), 3).alias("distinct_ids_per_title_len"),
        F.when(F.col("title_len") >= 11, "large").when(F.col("title_len") >= 7, "medium").otherwise("small").alias("title_len_tier"),
        (F.col("title_len") + F.col("distinct_ids")).alias("title_len_plus_distinct_ids"),
    )
)

sequenced = (
    annotated
    .select(
        "title_len",
        "n_rows",
        "title_len_upper_trim_title_like_g",
        "title_len_tier",
        "title_len_plus_distinct_ids",
        F.row_number().over(w_all_by_n_rows).alias("rn"),
        F.count(F.lit(1)).over(w_title_len_tier).alias("rows_in_title_len_tier"),
    )
)

qualified = (
    sequenced
    .crossJoin(sequenced.agg(F.avg("title_len").alias("overall_avg_title_len")))
    .filter(F.col("title_len") > F.col("overall_avg_title_len"))
    .drop("overall_avg_title_len")
)

with_windows = (
    qualified
    .select(
        "*",
        F.round(F.col("n_rows") - F.avg("n_rows").over(w_rn_n_rows), 2).alias("n_rows_vs_rn_n_rows_avg"),
        F.min("rows_in_title_len_tier").over(w_rn_title_len_by_n_rows_runni).alias("min_rows_in_title_len_tier_so_far"),
        F.round(F.cume_dist().over(w_n_rows_by_rows_in_title_len), 3).alias("rows_in_title_len_tier_cume_dist"),
        F.dense_rank().over(w_title_len_by_rows_in_title_l).alias("rows_in_title_len_tier_dense_rank"),
        F.count(F.lit(1)).over(w_rn_n_rows).alias("rows_in_rn_n_rows"),
        F.lag("title_len", 3).over(w_rn_title_len_by_n_rows).alias("prev3_title_len"),
    )
)

result = (
    with_windows
    .select(
        "title_len",
        "title_len_upper_trim_title_like_g",
        "title_len_tier",
        "title_len_plus_distinct_ids",
        "rn",
        "rows_in_title_len_tier",
        "min_rows_in_title_len_tier_so_far",
        "rows_in_title_len_tier_dense_rank",
        "rows_in_rn_n_rows",
        "prev3_title_len",
    )
    .orderBy(F.col("rn").asc(), F.col("rows_in_title_len_tier").desc())
)
