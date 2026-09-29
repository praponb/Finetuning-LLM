from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), F.col("id").desc())
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id")
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("E%"))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
        F.add_months(F.col("release_date"), 12).alias("release_date_plus_12m"),
    )
)

latest_per_key = (
    valid
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

windowed = (
    latest_per_key
    .select(
        "*",
        F.lag("release_date_plus_12m", 2).over(w_is_explicit_by_release_date).alias("prev2_release_date_plus_12m"),
        F.dense_rank().over(w_is_explicit_by_release_date).alias("release_date_dense_rank"),
        F.nth_value("release_date_plus_12m", 2).over(w_is_explicit_by_release_date_2).alias("second_release_date_plus_12m"),
    )
)

result = (
    windowed
    .groupingSets(
        [["is_explicit", "release_date_dense_rank", "artist_id"], ["is_explicit"], ["release_date_dense_rank"], []],
        "is_explicit", "release_date_dense_rank", "artist_id",
    )
    .agg(
        F.sum("release_date_dense_rank").alias("total_release_date_dense_rank"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("release_date_dense_rank").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
