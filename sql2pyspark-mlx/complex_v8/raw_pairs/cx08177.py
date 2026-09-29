from pyspark.sql import functions as F

filtered_listeners = (
    df_listeners
    .filter(F.col("country") != "UK")
    .select(
        "id",
        "username",
        "country",
        "birth_year",
        F.trunc(F.col("signup_date"), "MM").alias("signup_date_month_start"),
    )
)

listener_rows = (
    filtered_listeners
    .select(
        F.lit("listener").alias("src"),
        F.col("id").alias("qty"),
        F.col("signup_date_month_start").alias("day"),
        "country",
    )
)

listener_rows_2 = (
    df_listeners
    .select(
        F.lit("listener_2").alias("src"),
        "birth_year",
        "signup_date",
        "country",
    )
)

all_events = listener_rows.union(listener_rows_2)

result = (
    all_events
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("day").alias("latest_day"),
    )
    .orderBy("src")
)
