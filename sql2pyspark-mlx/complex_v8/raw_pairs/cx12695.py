from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter((F.length(F.trim(F.col("username"))) >= 6) & (~F.array_contains(F.col("interests"), "jazz")))
    .select("id")
)

listener_side_2 = df_listeners.select("birth_year")

result = (
    listener_side
    .subtract(listener_side_2)
    .orderBy("id")
)
