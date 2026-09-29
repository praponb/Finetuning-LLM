from pyspark.sql import functions as F

artist_rows = df_artists.select("debut_year", "country")

listener_side = (
    df_listeners
    .filter((~F.col("country").isin("KR", "JP")) & (F.size(F.col("interests")) >= 1))
    .select("id", "username")
)

result = (
    artist_rows
    .subtract(listener_side)
    .orderBy(F.col("country").desc())
)
