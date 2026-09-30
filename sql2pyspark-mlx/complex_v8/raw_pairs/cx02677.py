from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("plan").orderBy(F.col("birth_year").desc(), F.col("id").desc())

scoped = (
    df_listeners
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.date_trunc("quarter", F.col("signup_date")).alias("signup_date_quarter"),
    )
)

deduped = (
    scoped
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

exploded_detail = (
    deduped.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.username",
        "s.signup_date",
        "s.birth_year",
        "s.interests",
        "s.signup_date_quarter",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
    )
)

exploded = (
    exploded_detail
    .select(
        "id",
        "username",
        "signup_date",
        "birth_year",
        "signup_date_quarter",
        "play_id",
        "play_ms_played",
        "play_source",
        F.explode_outer("interests").alias("interest"),
    )
)

result = (
    exploded
    .filter(F.col("play_ms_played") < 324437)
    .select(
        "id",
        "username",
        "birth_year",
        "play_id",
        "play_ms_played",
        "play_source",
        "interest",
        F.round(F.col("birth_year") / F.nullif(F.col("play_ms_played"), F.lit(0)), 3).alias("birth_year_per_play_ms_played"),
        F.concat_ws(" / ", F.col("interest"), F.col("username")).alias("interest_username_label"),
    )
    .orderBy(F.col("birth_year").desc(), F.col("username").desc())
)
