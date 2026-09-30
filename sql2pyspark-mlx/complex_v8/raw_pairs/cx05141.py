from pyspark.sql import functions as F

scoped_listeners = (
    df_listeners
    .filter((F.col("signup_date").between(F.lit("2024-04-16").cast("date"), F.lit("2025-01-29").cast("date"))) & (F.col("plan").isin("student", "premium")))
    .select(
        "id",
        "country",
        "signup_date",
        "birth_year",
        F.date_format(F.col("signup_date"), "MMM yyyy").alias("signup_date_label"),
    )
)

listener_side = (
    scoped_listeners
    .filter(F.col("country") != "JP")
    .select("birth_year")
)

play_rows = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("source").isin("search", "playlist")))
    .select("id")
)

result = (
    listener_side
    .subtract(play_rows)
    .orderBy("birth_year")
)
