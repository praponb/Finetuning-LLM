from pyspark.sql import functions as F

scoped_listeners = (
    df_listeners
    .filter(F.col("signup_date").between(F.lit("2024-07-13").cast("date"), F.lit("2025-09-26").cast("date")))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
    )
)

listener_side = (
    scoped_listeners
    .select(F.col("id").alias("entity_id"), F.col("plan").alias("category"))
)

track_side = (
    df_tracks
    .filter((F.col("duration_sec") < 309) & (F.length(F.trim(F.col("title"))) >= 5))
    .select(F.col("id").alias("entity_id"), F.col("title").alias("category"))
)

unioned = listener_side.unionByName(track_side)

result = (
    unioned
    .groupBy("category")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
    )
    .orderBy("category")
)
