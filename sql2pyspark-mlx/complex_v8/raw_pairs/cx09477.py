from pyspark.sql import functions as F

base = (
    df_tracks
    .filter((F.col("release_date") >= F.lit("2024-07-01").cast("date")) & (F.lower(F.col("title")).like("%ai%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        F.last_day(F.col("release_date")).alias("release_date_month_end"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
    )
)

scored = (
    base
    .filter(F.col("release_date_month_end") >= F.lit("2024-12-01").cast("date"))
    .select(
        "*",
        F.year(F.col("release_date_month_end")).alias("release_date_month_end_year"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

result = (
    scored
    .select("id", "bpm", "is_explicit_flag")
    .unpivot(
        ids=["id"],
        values=["bpm", "is_explicit_flag"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
