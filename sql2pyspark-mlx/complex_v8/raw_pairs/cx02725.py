from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_is_explic = Window.partitionBy(F.month("release_date"), "is_explicit")
w_release_date_month_is_explic_2 = Window.partitionBy(F.month("release_date"), "is_explicit").orderBy(F.col("release_date").desc(), "id")
w_release_date_month_is_explic_3 = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit").orderBy("avg_bpm", "id", "mood").rowsBetween(-6, -1)
w_release_date_month_is_explic_4 = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit").orderBy("avg_bpm", "id", "mood").rowsBetween(-1, Window.currentRow)

filtered_tracks = df_tracks

with_windows = (
    filtered_tracks
    .withColumn("bpm_share_of_release_date_month_is_explicit", F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_release_date_month_is_explic), F.lit(0)), 4))
    .withColumn("next_release_date", F.lead("release_date").over(w_release_date_month_is_explic_2))
)

artist_id_stats = (
    with_windows
    .groupBy("artist_id")
    .agg(F.avg("bpm").alias("avg_bpm"))
)

above_average = (
    with_windows.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") > F.col("g.avg_bpm"))
    .select(
        "s.*",
        "g.avg_bpm",
        F.round(F.col("s.bpm") - F.col("g.avg_bpm"), 2).alias("bpm_minus_avg"),
    )
)

flattened = (
    above_average
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "bpm_share_of_release_date_month_is_explicit",
        "next_release_date",
        "avg_bpm",
        "bpm_minus_avg",
        F.explode("moods").alias("mood"),
    )
)

ranked = (
    flattened
    .select(
        "*",
        F.max("avg_bpm").over(w_release_date_month_is_explic_3).alias("avg_bpm_max_6_prior"),
        F.round(F.avg("bpm_minus_avg").over(w_release_date_month_is_explic_4), 2).alias("bpm_minus_avg_avg_2"),
    )
)

result = (
    ranked
    .select(
        "id",
        "mood",
        F.col("bpm_minus_avg").alias("bpm_minus_avg_v"),
        F.col("avg_bpm_max_6_prior").alias("m_avg_bpm_max_6_prior"),
        F.col("bpm_minus_avg_avg_2").alias("m_bpm_minus_avg_avg_2"),
    )
    .unpivot(
        ids=["id", "mood"],
        values=["bpm_minus_avg_v", "m_avg_bpm_max_6_prior", "m_bpm_minus_avg_avg_2"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .orderBy("id", "mood", "metric_name")
)
