from pyspark.sql import functions as F

play_rows = (
    df_plays
    .filter((F.col("device") == "phone") & (~F.col("is_skipped")))
    .select(F.col("id").alias("qty"), F.col("device").alias("name"))
)

listener_rows = (
    df_listeners
    .filter(F.col("signup_date") >= F.lit("2024-10-07").cast("date"))
    .select(F.col("birth_year").alias("qty"), F.col("username").alias("name"))
)

result = play_rows.intersect(listener_rows)
