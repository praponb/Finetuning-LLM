from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .select(
        F.col("plan").alias("category"),
        "signup_date",
        F.col("birth_year").alias("entity_id"),
        F.lit("listener").alias("source"),
    )
)

listener_side_2 = (
    df_listeners
    .select(
        "username",
        "signup_date",
        "birth_year",
        F.lit("listener_2").alias("source"),
    )
)

result = (
    listener_side
    .union(listener_side_2)
    .orderBy("signup_date", "category")
)
