from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter(F.col("birth_year").between(1969, 1981))
    .select("country", "username")
)

play_rows = (
    df_plays
    .filter((F.col("listener_id") >= 3) & (F.col("device").isin("phone", "speaker")))
    .select("device", "source")
)

result = (
    listener_side
    .union(play_rows)
    .distinct()
    .orderBy("country")
)
