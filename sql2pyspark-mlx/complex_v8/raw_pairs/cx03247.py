from pyspark.sql import functions as F

cleaned = (
    df_listeners
    .filter(~F.array_contains(F.col("interests"), "podcasts"))
    .select(
        "id",
        "username",
        "country",
        "birth_year",
        (F.floor(F.col("birth_year") / 10) * 10).alias("birth_year_bucket"),
    )
)

listener_rows = (
    cleaned
    .filter((F.col("birth_year_bucket") > 1979) & (F.col("birth_year") <= 1980))
    .select(F.col("username").alias("category"))
)

listener_side = (
    df_listeners
    .filter((F.col("birth_year") < 1986) & (F.upper(F.trim(F.col("username"))).like("E%")))
    .select("username")
)

result = (
    listener_rows
    .subtract(listener_side)
    .orderBy("category")
)
