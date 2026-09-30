from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter((F.year(F.col("signup_date")) == 2024) & (F.col("birth_year") < 1994))
    .select("birth_year", "signup_date", F.lit("listener").alias("src"))
)

listener_side_2 = (
    df_listeners
    .filter(F.array_contains(F.col("interests"), "indie"))
    .select("birth_year", "signup_date", F.lit("listener_2").alias("src"))
)

listener_rows = df_listeners.select("birth_year", "signup_date", F.lit("listener_3").alias("src"))

result = (
    listener_side
    .unionByName(listener_side_2)
    .unionByName(listener_rows)
    .orderBy(F.col("birth_year").desc())
)
