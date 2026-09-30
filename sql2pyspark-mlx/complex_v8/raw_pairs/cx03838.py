from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .select(
        F.lit("listener").alias("src"),
        F.col("username").alias("name"),
        F.col("signup_date").alias("event_date"),
        "country",
    )
)

listener_side = (
    df_listeners
    .select(
        F.lit("listener_2").alias("src"),
        "plan",
        "signup_date",
        "country",
    )
)

result = (
    listener_rows
    .union(listener_side)
    .distinct()
)
