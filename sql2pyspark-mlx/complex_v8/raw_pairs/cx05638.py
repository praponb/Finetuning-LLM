from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("listener_id").orderBy("prior_ms_played_7d", "id")

joined_ranges = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.ms_played").alias("prior_ms_played_7d"),
    )
)

latest = (
    joined_ranges
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

joined = (
    latest.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.prior_rows_7d",
        "s.prior_ms_played_7d",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

result = (
    joined
    .select(
        "id",
        "prior_rows_7d",
        "prior_ms_played_7d",
        "listener_signup_date",
        "listener_birth_year",
        (F.floor(F.col("prior_ms_played_7d") / 1000000) * 1000000).alias("prior_ms_played_7d_bucket"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
