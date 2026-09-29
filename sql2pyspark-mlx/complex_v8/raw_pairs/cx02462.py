from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_n_rows_by_n_rows = Window.partitionBy("is_explicit", "n_rows").orderBy(F.col("n_rows").desc(), "is_explicit")
w_is_explicit_n_rows_by_n_rows_2 = Window.partitionBy("is_explicit", "n_rows").orderBy("n_rows")
w_n_rows = Window.partitionBy("n_rows")
w_n_rows_seq = Window.partitionBy("n_rows_seq")
w_is_explicit_by_n_rows_avg_n = Window.partitionBy("is_explicit").orderBy(F.col("n_rows_avg_n_rows").desc())
w_is_explicit_by_n_rows_avg_n_2 = Window.partitionBy("is_explicit").orderBy(F.col("n_rows_avg_n_rows").desc(), "is_explicit").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_rows_dense_rank_is_explici = Window.partitionBy("n_rows_dense_rank", "is_explicit").orderBy(F.col("first_n_rows_dense_rank").desc(), "is_explicit")
w_is_explicit_by_n_rows_vs_n_r = Window.partitionBy("is_explicit").orderBy("n_rows_vs_n_rows_seq_avg", "is_explicit")
w_is_explicit_by_first_n_rows = Window.partitionBy("is_explicit").orderBy(F.col("first_n_rows_dense_rank").desc())
w_n_rows_dense_rank_is_explici_2 = Window.partitionBy("n_rows_dense_rank", "is_explicit").orderBy(F.col("first_n_rows_dense_rank").desc(), "is_explicit").rowsBetween(-2, -1)

base = (
    df_tracks
    .filter(F.col("bpm") > 104)
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
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

rollup_base = (
    base
    .groupBy("is_explicit")
    .agg(
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
    )
    .filter(F.col("n_rows") >= 3)
)

ranked = (
    rollup_base
    .select(
        "*",
        F.row_number().over(w_is_explicit_n_rows_by_n_rows).alias("n_rows_seq"),
        F.dense_rank().over(w_is_explicit_n_rows_by_n_rows_2).alias("n_rows_dense_rank"),
        F.round(F.avg("n_rows").over(w_n_rows), 2).alias("n_rows_avg_n_rows"),
    )
)

with_trends = (
    ranked
    .select(
        "is_explicit",
        "title_list",
        "n_rows_dense_rank",
        "n_rows_avg_n_rows",
        F.round(F.col("n_rows_dense_rank") / F.nullif(F.sum("n_rows_dense_rank").over(w_n_rows_seq), F.lit(0)), 4).alias("n_rows_dense_rank_share_of_n_rows_seq"),
        F.rank().over(w_is_explicit_by_n_rows_avg_n).alias("n_rows_avg_n_rows_rank"),
        F.first("n_rows_dense_rank").over(w_is_explicit_by_n_rows_avg_n_2).alias("first_n_rows_dense_rank"),
        F.round(F.col("n_rows") - F.avg("n_rows").over(w_n_rows_seq), 2).alias("n_rows_vs_n_rows_seq_avg"),
    )
)

ranked_2 = (
    with_trends
    .select(
        "*",
        (F.col("n_rows_avg_n_rows") - F.lag("n_rows_avg_n_rows").over(w_n_rows_dense_rank_is_explici)).alias("n_rows_avg_n_rows_change"),
        F.lead("n_rows_dense_rank").over(w_is_explicit_by_n_rows_vs_n_r).alias("next_n_rows_dense_rank"),
        F.round(F.percent_rank().over(w_is_explicit_by_first_n_rows), 3).alias("first_n_rows_dense_rank_percent_rank"),
        F.round(F.avg("first_n_rows_dense_rank").over(w_n_rows_dense_rank_is_explici_2), 2).alias("first_n_rows_dense_rank_avg_2_prior"),
    )
)

result = (
    ranked_2
    .select(
        "is_explicit",
        "n_rows_dense_rank",
        "n_rows_avg_n_rows",
        "n_rows_avg_n_rows_rank",
        "first_n_rows_dense_rank",
        "next_n_rows_dense_rank",
    )
    .orderBy(
        F.col("next_n_rows_dense_rank").desc(),
        F.col("n_rows_avg_n_rows_rank").asc_nulls_last(),
    )
)
