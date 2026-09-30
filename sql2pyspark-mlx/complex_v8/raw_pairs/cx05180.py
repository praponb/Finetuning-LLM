from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("team_or_unknown").orderBy(F.col("greatest_level_adjuster_name_len").desc(), F.col("id").desc())

recent_adjusters = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len"),
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug_2"),
    )
)

scored = (
    recent_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        "adjuster_name_len",
        "team_slug",
        F.abs(F.col("adjuster_name_len") - 7).alias("adjuster_name_len_distance"),
        F.greatest(F.col("level"), F.col("adjuster_name_len")).alias("greatest_level_adjuster_name_len"),
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
    )
)

latest_per_key = (
    scored
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

derived = (
    latest_per_key
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("team_or_unknown")), " ", "_")).alias("team_or_unknown_slug"),
        F.length(F.trim(F.col("team"))).alias("team_len"),
    )
)

result = (
    derived
    .groupBy("team_len")
    .pivot("team", ["Travel", "Property"])
    .agg(F.max("adjuster_name_len"))
    .orderBy("team_len")
)
