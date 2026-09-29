from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_birth_year = Window.orderBy("birth_year", "id", "interest_pos")
w_country_or_unknown_by_birth = Window.partitionBy("country_or_unknown").orderBy("birth_year", "id", "interest_pos")
w_interest = Window.partitionBy("interest")

filtered = (
    df_listeners
    .filter(F.col("birth_year") >= 1968)
    .select(
        "id",
        "username",
        "plan",
        "birth_year",
        "interests",
        F.abs(F.col("birth_year") - 1981).alias("birth_year_distance"),
        F.abs(F.col("birth_year") - 1975).alias("birth_year_distance_2"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

summarized_rows = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select("s.*", "p.first_played_at")
)

enriched = (
    summarized_rows
    .select(
        "*",
        F.lower(F.trim(F.col("country_or_unknown"))).alias("country_or_unknown_clean"),
    )
)

tagged = (
    enriched
    .select(
        "id",
        "birth_year",
        "interests",
        "birth_year_distance",
        "country_or_unknown",
        "first_played_at",
        F.substring(F.trim(F.col("plan")), 1, 3).alias("plan_prefix"),
    )
)

flattened = (
    tagged
    .select(
        "id",
        "birth_year",
        "birth_year_distance",
        "country_or_unknown",
        "first_played_at",
        "plan_prefix",
        F.posexplode_outer("interests").alias("interest_pos", "interest"),
    )
)

with_activity = (
    flattened
    .crossJoin(flattened.agg(F.avg("birth_year_distance").alias("overall_avg_birth_year_distance")))
    .filter(F.col("birth_year_distance") > F.col("overall_avg_birth_year_distance"))
    .drop("overall_avg_birth_year_distance")
)

metrics = (
    with_activity
    .select(
        "*",
        (F.col("birth_year") - F.lag("birth_year").over(w_all_by_birth_year)).alias("birth_year_change"),
        F.lag("birth_year").over(w_country_or_unknown_by_birth).alias("prev_birth_year"),
        F.round(F.col("birth_year_distance") - F.avg("birth_year_distance").over(w_interest), 2).alias("birth_year_distance_vs_interest_avg"),
    )
)

result = (
    metrics
    .select(
        "id",
        "birth_year_distance",
        "country_or_unknown",
        "plan_prefix",
        "interest",
        "birth_year_change",
        "prev_birth_year",
        F.greatest(F.col("birth_year"), F.col("birth_year_change")).alias("greatest_birth_year_birth_year_change"),
    )
    .orderBy(F.col("prev_birth_year").desc(), F.col("birth_year_distance").asc())
)
