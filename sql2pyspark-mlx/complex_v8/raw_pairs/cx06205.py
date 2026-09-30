from pyspark.sql import functions as F

scoped_listeners = (
    df_listeners
    .filter((F.col("birth_year") < 1995) | (F.col("signup_date") >= F.lit("2024-08-02").cast("date")))
    .select(
        "id",
        "username",
        "country",
        "birth_year",
    )
)

listener_rows = scoped_listeners.select(F.col("id").alias("qty"), "birth_year")

listener_rows_2 = (
    df_listeners
    .filter((F.col("signup_date").between(F.lit("2024-07-13").cast("date"), F.lit("2025-03-18").cast("date"))) & (F.lower(F.col("username")).like("%ch%")))
    .select(F.col("birth_year").alias("qty"), F.col("id").alias("birth_year"))
)

play_rows = (
    df_plays
    .filter((F.col("ms_played") > 71476) & (F.col("listener_id") >= 16))
    .select(F.col("track_id").alias("qty"), F.col("ms_played").alias("birth_year"))
)

unioned = (
    listener_rows
    .union(listener_rows_2)
    .union(play_rows)
)

result = (
    unioned
    .groupBy("qty")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("birth_year").alias("total_birth_year"),
    )
    .orderBy("qty")
)
