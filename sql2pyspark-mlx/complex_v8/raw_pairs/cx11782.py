from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("country", "plan").orderBy(F.col("birth_year").desc(), F.col("id").desc())

cleaned_listeners = (
    df_listeners
    .filter((F.col("username").isNotNull()) & (F.col("birth_year").between(1966, 1986)))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.date_add(F.col("signup_date"), 30).alias("signup_date_plus_30d"),
    )
)

unique_rows = (
    cleaned_listeners
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

derived = (
    unique_rows
    .filter(F.upper(F.trim(F.col("username"))).like("D%"))
    .select(
        "id",
        "username",
        "signup_date",
        "birth_year",
        F.date_format(F.col("signup_date_plus_30d"), "yyyy-MM-dd").alias("signup_date_plus_30d_label"),
        F.datediff(F.col("signup_date_plus_30d"), F.col("signup_date")).alias("days_signup_date_to_signup_date_plus_30d"),
        (F.floor(F.col("birth_year") / 10) * 10).alias("birth_year_bucket"),
    )
)

screened = (
    derived
    .crossJoin(derived.agg(F.avg("days_signup_date_to_signup_date_plus_30d").alias("overall_avg_days_signup_date_to_signup_date_plus")))
    .filter(F.col("days_signup_date_to_signup_date_plus_30d") > F.col("overall_avg_days_signup_date_to_signup_date_plus"))
    .drop("overall_avg_days_signup_date_to_signup_date_plus")
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

with_counts = (
    screened.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select("s.*", "p.last_played_at")
)

result = (
    with_counts
    .select(
        "id",
        "username",
        "signup_date",
        "birth_year",
        "signup_date_plus_30d_label",
        "days_signup_date_to_signup_date_plus_30d",
        "birth_year_bucket",
        "last_played_at",
        F.abs(F.col("birth_year_bucket") - 1991).alias("birth_year_bucket_distance"),
    )
    .orderBy(F.col("birth_year").asc_nulls_last(), "id")
)
