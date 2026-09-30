from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter((F.size(F.col("interests")) >= 1) & (F.col("signup_date").between(F.lit("2024-07-13").cast("date"), F.lit("2025-01-29").cast("date"))))
    .select(
        F.col("id").alias("ref_id"),
        F.col("birth_year").alias("entity_id"),
        "signup_date",
    )
)

track_rows = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.col("duration_sec") > 165))
    .select(
        F.col("artist_id").alias("ref_id"),
        F.col("duration_sec").alias("entity_id"),
        F.col("release_date").alias("signup_date"),
    )
)

unioned = listener_side.union(track_rows)

result = (
    unioned
    .groupBy("ref_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
        F.max("signup_date").alias("latest_signup_date"),
    )
    .orderBy("ref_id")
)
