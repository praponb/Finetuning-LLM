from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter((F.col("birth_year") <= 1990) & (F.col("signup_date").between(F.lit("2024-04-16").cast("date"), F.lit("2025-05-05").cast("date"))))
    .select(F.col("country").alias("label"))
)

listener_side_2 = (
    df_listeners
    .filter(F.array_contains(F.col("interests"), "podcasts"))
    .select("country")
)

result = (
    listener_side
    .exceptAll(listener_side_2)
    .orderBy("label")
)
