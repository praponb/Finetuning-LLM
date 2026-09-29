from pyspark.sql import functions as F

listener_rows = (
    df_listeners
    .select(
        F.col("username").alias("name"),
        F.col("signup_date").alias("event_date"),
    )
)

listener_rows_2 = (
    df_listeners
    .select(F.col("plan").alias("name"), F.col("signup_date").alias("event_date"))
)

unioned = listener_rows.union(listener_rows_2)

result = (
    unioned
    .groupBy("name")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("name")
)
