from pyspark.sql import functions as F

prepared = (
    df_listeners
    .filter((F.col("signup_date").between(F.lit("2024-08-01").cast("date"), F.lit("2025-07-08").cast("date"))) | (F.upper(F.trim(F.col("username"))).like("A%")))
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.last_day(F.col("signup_date")).alias("signup_date_month_end"),
    )
)

listener_rows = (
    prepared
    .filter((F.lower(F.col("username")).like("%99%")) & (F.array_contains(F.col("interests"), "jazz")))
    .select(
        F.col("country").alias("label"),
        F.col("username").alias("name"),
        F.lit("listener").alias("record_type"),
    )
)

artist_rows = (
    df_artists
    .filter(F.col("country") == "US")
    .select(
        F.col("artist_name").alias("label"),
        F.col("country").alias("name"),
        F.lit("artist").alias("record_type"),
    )
)

result = listener_rows.unionByName(artist_rows)
