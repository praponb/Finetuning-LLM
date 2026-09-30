from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_signup_date_prior_1 = Window.orderBy("signup_date", "id").rowsBetween(-1, -1)
w_all_by_signup_date = Window.orderBy("signup_date", "id")
w_interest_by_birth_year_max_1 = Window.partitionBy("interest").orderBy(F.col("birth_year_max_1_prior").desc(), "id", "interest")
w_interest_last_played_at_to_d = Window.partitionBy("interest", F.to_date("last_played_at"))

recent_listeners = (
    df_listeners
    .filter((F.col("birth_year") >= 1975) & (F.lower(F.col("username")).like("%_m%")))
    .select(
        "id",
        "username",
        "signup_date",
        "birth_year",
        "interests",
    )
)

with_windows = (
    recent_listeners
    .withColumn("birth_year_max_1_prior", F.max("birth_year").over(w_all_by_signup_date_prior_1))
    .withColumn("next2_birth_year", F.lead("birth_year", 2).over(w_all_by_signup_date))
)

exploded = (
    with_windows
    .select(
        "id",
        "username",
        "signup_date",
        "birth_year",
        "birth_year_max_1_prior",
        "next2_birth_year",
        F.explode("interests").alias("interest"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

with_stats = (
    exploded.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select("s.*", "p.last_played_at")
)

sequenced = (
    with_stats
    .select(
        "*",
        F.dense_rank().over(w_interest_by_birth_year_max_1).alias("birth_year_max_1_prior_dense_rank"),
        F.lead("next2_birth_year", 2).over(w_interest_by_birth_year_max_1).alias("next2_next2_birth_year"),
        F.count(F.lit(1)).over(w_interest_last_played_at_to_d).alias("rows_in_interest_last_played_at_to_date"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["interest", "birth_year_max_1_prior_dense_rank"], ["interest"], []],
        "interest", "birth_year_max_1_prior_dense_rank",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("birth_year") / F.nullif(F.sum("rows_in_interest_last_played_at_to_date"), F.lit(0)), 3).alias("birth_year_to_rows_in_interest_last_played_at_to"),
        F.max("signup_date").alias("last_signup_date"),
        F.sum(F.when(F.length(F.trim(F.col("username"))) >= 7, F.col("birth_year_max_1_prior"))).alias("birth_year_max_1_prior_length_trim_username_7"),
    )
    .select(
        F.coalesce(F.col("interest"), F.lit("ALL")).alias("interest"),
        "birth_year_max_1_prior_dense_rank",
        "grouping_level",
        "birth_year_to_rows_in_interest_last_played_at_to",
        "last_signup_date",
        "birth_year_max_1_prior_length_trim_username_7",
    )
    .orderBy(
        F.col("interest").asc_nulls_last(),
        F.col("birth_year_max_1_prior_dense_rank").asc_nulls_last(),
    )
)
