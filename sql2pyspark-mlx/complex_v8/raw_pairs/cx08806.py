from pyspark.sql import functions as F

base = (
    df_listeners
    .filter((F.col("birth_year") <= 1994) & (F.col("country").isin("TH", "BR", "KR")))
    .select(
        "id",
        "plan",
        "signup_date",
        "interests",
    )
)

listener_side = (
    df_listeners
    .filter(F.col("country") == "TH")
    .select("plan", F.col("country").alias("category"))
)

artist_side = df_artists.select("genre", "country")

combined = listener_side.union(artist_side)

result = (
    combined
    .groupBy("plan")
    .agg(F.count("*").alias("n_rows"))
    .orderBy("plan")
)
