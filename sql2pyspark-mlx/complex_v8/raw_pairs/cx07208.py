from pyspark.sql import functions as F

filtered = (
    df_listeners
    .filter((~F.array_contains(F.col("interests"), "live")) | (F.col("country").isin("JP", "BR", "UK")))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "interests",
        F.year(F.col("signup_date")).alias("signup_date_year"),
    )
)

listener_rows = (
    filtered
    .filter(F.col("plan") == "premium")
    .select(F.col("id").alias("key_id"), F.col("plan").alias("label"))
)

artist_side = (
    df_artists
    .filter((F.col("debut_year") > 1991) & (F.col("genre") == "Classical"))
    .select("debut_year", "artist_name")
)

result = listener_rows.subtract(artist_side)
