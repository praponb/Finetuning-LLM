from pyspark.sql import functions as F

listener_side = (
    df_listeners
    .select(
        F.col("plan").alias("label"),
        F.col("username").alias("category"),
        F.col("signup_date").alias("day"),
    )
)

listener_side_2 = df_listeners.select("plan", "country", "signup_date")

unioned = listener_side.union(listener_side_2)

result = (
    unioned
    .groupBy("label")
    .agg(F.count("*").alias("n_rows"), F.max("day").alias("latest_day"))
    .orderBy("label")
)
