from pyspark.sql import functions as F

play_side = (
    df_plays
    .filter((F.col("device") == "desktop") & (F.month(F.col("played_at")).isin(2, 3)))
    .select("id", "source")
)

listener_side = (
    df_listeners
    .filter((F.col("country").isin("TH", "JP", "BR")) & (F.col("signup_date").isNotNull()))
    .select(F.col("birth_year").alias("id"), F.col("country").alias("source"))
)

result = (
    play_side
    .intersectAll(listener_side)
    .orderBy("id", "source")
)
