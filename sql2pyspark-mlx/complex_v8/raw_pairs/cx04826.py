from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .filter((F.col("birth_year") < 1986) & (F.col("plan").isin("premium", "student", "free")))
    .select(
        "birth_year",
        F.col("signup_date").alias("event_date"),
        F.lit("listener").alias("origin"),
    )
)

listener_side_2 = df_listeners.select("id", "signup_date", F.lit("listener_2").alias("origin"))

unioned = listener_side.union(listener_side_2)

result = (
    unioned
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("birth_year").alias("total_birth_year"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("origin")
)
