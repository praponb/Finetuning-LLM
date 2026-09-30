from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .select(
        "plan",
        "birth_year",
        "username",
        F.lit("listener").alias("origin"),
    )
)

listener_rows = (
    df_listeners
    .filter(F.size(F.col("interests")) >= 1)
    .select(
        "plan",
        "id",
        "country",
        F.lit("listener_2").alias("origin"),
    )
)

all_events = listener_side.union(listener_rows)

result = (
    all_events
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("birth_year").alias("total_birth_year"),
    )
    .orderBy("origin")
)
