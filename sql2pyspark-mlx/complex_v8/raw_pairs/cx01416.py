from pyspark.sql import functions as F

scoped = (
    df_listeners
    .filter((F.col("country").isNotNull()) & (F.col("signup_date") < F.lit("2025-01-29").cast("date")))
    .select(
        "id",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        F.date_trunc("year", F.col("signup_date")).alias("signup_date_year"),
    )
)

listener_side = scoped.select("birth_year", "signup_date")

track_rows = (
    df_tracks
    .filter((F.col("duration_sec") <= 352) & (F.col("artist_id").isNotNull()))
    .select(
        F.col("id").alias("birth_year"),
        F.col("release_date").alias("signup_date"),
    )
)

result = listener_side.union(track_rows)
