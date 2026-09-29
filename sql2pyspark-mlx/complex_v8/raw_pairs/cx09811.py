from pyspark.sql import functions as F

recent = (
    df_listeners
    .filter(F.col("birth_year") >= 1969)
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.array_contains(F.col("interests"), "live").alias("has_live"),
    )
)

listener_side = (
    recent
    .filter((F.col("country").isin("BR", "KR", "UK")) & (F.col("signup_date").between(F.lit("2024-03-21").cast("date"), F.lit("2025-03-18").cast("date"))))
    .select("id")
)

track_side = (
    df_tracks
    .filter(F.col("artist_id") >= 12)
    .select("id")
)

result = (
    listener_side
    .subtract(track_side)
    .orderBy(F.col("id").desc())
)
