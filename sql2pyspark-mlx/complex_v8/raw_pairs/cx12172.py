from pyspark.sql import functions as F

cleaned = (
    df_listeners
    .select(
        "id",
        "username",
        "signup_date",
        "birth_year",
        "interests",
        F.concat(F.col("plan"), F.lit("#"), F.col("birth_year").cast("string")).alias("plan_ref"),
    )
)

tagged = (
    cleaned
    .filter(F.size(F.col("interests")) >= 1)
    .select(
        "id",
        "signup_date",
        "interests",
        F.length(F.trim(F.col("plan_ref"))).alias("plan_ref_len"),
    )
)

filtered = (
    tagged.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left_semi")
)

result = (
    filtered
    .select("id")
    .orderBy("id")
    .limit(20)
)
