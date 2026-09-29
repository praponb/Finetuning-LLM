from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), F.col("id").desc())

prepared = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("G%")) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "moods",
    )
)

latest = (
    prepared
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

grouped = (
    latest
    .groupBy(
        "is_explicit",
        "artist_id",
        F.date_trunc("month", "release_date").alias("release_date_month"),
    )
    .agg(
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.count(F.when(F.upper(F.trim(F.col("title"))).like("S%"), 1)).alias("n_upper_trim_title_like_s"),
        F.count(F.when(F.col("title").isNotNull(), 1)).alias("n_title_is_not_null"),
        F.count("*").alias("row_count"),
    )
)

result = (
    grouped
    .select(
        "is_explicit",
        "artist_id",
        F.col("n_title_is_not_null").alias("m_n_title_is_not_null"),
        F.col("distinct_artist_ids").alias("distinct_artist_ids_v"),
        F.col("n_upper_trim_title_like_s").alias("n_upper_trim_title_like_s_v"),
    )
    .unpivot(
        ids=["is_explicit", "artist_id"],
        values=["m_n_title_is_not_null", "distinct_artist_ids_v", "n_upper_trim_title_like_s_v"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("is_explicit", "artist_id", "field")
)
