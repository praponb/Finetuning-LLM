from pyspark.sql import functions as F

prepared = (
    df_listeners
    .filter(F.col("country") == "UK")
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
    )
)

listener_rows = prepared.select(F.col("id").alias("entity_id"), "plan")

play_rows = df_plays.select("track_id", "source")

result = (
    listener_rows
    .union(play_rows)
    .distinct()
    .orderBy(F.col("entity_id").desc(), "plan")
)
