from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_verified_flag_is_verified = Window.partitionBy("is_verified_flag", "is_verified").orderBy(F.col("is_verified_flag").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_all_by_is_verified_flag = Window.orderBy(F.col("is_verified_flag").desc(), "id")
w_is_verified_flag_genre_or_un = Window.partitionBy("is_verified_flag", "genre_or_unknown")
w_all_by_is_verified_flag_prio = Window.orderBy(F.col("is_verified_flag").desc(), "id").rowsBetween(-1, -1)
w_n_rows = Window.partitionBy("n_rows")
w_is_verified_n_rows_by_avg_is = Window.partitionBy("is_verified", "n_rows").orderBy("avg_is_verified_flag")

filtered = (
    df_artists
    .filter(F.col("genre").isin("Pop", "Jazz", "Indie"))
    .select(
        "id",
        "artist_name",
        "genre",
        "is_verified",
        F.upper(F.trim(F.col("genre"))).alias("genre_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("country")), " ", "_")).alias("country_slug"),
    )
)

annotated = (
    filtered
    .select(
        "id",
        "artist_name",
        "is_verified",
        "genre_clean",
        "country_slug",
        F.coalesce(F.col("genre"), F.lit("UNKNOWN")).alias("genre_or_unknown"),
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
    )
)

windowed = (
    annotated
    .select(
        "id",
        "is_verified",
        "country_slug",
        "genre_or_unknown",
        "is_verified_flag",
        F.sum("is_verified_flag").over(w_is_verified_flag_is_verified).alias("is_verified_flag_sum_2_prior"),
        F.lead("is_verified_flag").over(w_all_by_is_verified_flag).alias("next_is_verified_flag"),
        F.round(F.col("is_verified_flag") / F.nullif(F.sum("is_verified_flag").over(w_is_verified_flag_genre_or_un), F.lit(0)), 4).alias("is_verified_flag_share_of_is_verified_flag_genre"),
        F.lag("is_verified_flag", 1, 0).over(w_all_by_is_verified_flag).alias("prev_is_verified_flag"),
        F.row_number().over(w_all_by_is_verified_flag).alias("seq_no"),
        F.round(F.avg("is_verified_flag").over(w_all_by_is_verified_flag_prio), 2).alias("is_verified_flag_avg_1_prior"),
    )
)

stats = (
    windowed
    .filter(F.col("seq_no") > 5)
    .groupBy("is_verified", "seq_no")
    .agg(
        F.round(F.avg("is_verified_flag"), 2).alias("avg_is_verified_flag"),
        F.count("*").alias("n_rows"),
        F.max("is_verified_flag_sum_2_prior").alias("max_is_verified_flag_sum_2_prior_check"),
    )
    .filter(F.col("max_is_verified_flag_sum_2_prior_check") > 1)
    .drop("max_is_verified_flag_sum_2_prior_check")
)

ranked = (
    stats
    .select(
        "*",
        F.count(F.lit(1)).over(w_n_rows).alias("rows_in_n_rows"),
        F.sum("n_rows").over(w_n_rows).alias("n_rows_sum_n_rows"),
        F.round(F.cume_dist().over(w_is_verified_n_rows_by_avg_is), 3).alias("avg_is_verified_flag_cume_dist"),
    )
)

result = (
    ranked
    .select(
        "is_verified",
        "seq_no",
        "n_rows",
        "rows_in_n_rows",
        "n_rows_sum_n_rows",
        "avg_is_verified_flag_cume_dist",
    )
    .orderBy(F.col("rows_in_n_rows").desc())
)
