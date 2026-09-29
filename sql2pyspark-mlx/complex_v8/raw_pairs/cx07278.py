from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("track_id", "listener_id").orderBy(F.col("ms_played").desc(), F.col("id").desc())

valid = df_plays.filter(F.col("ms_played") < 340114)

unique_rows = (
    valid
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

filtered = (
    unique_rows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.source").isNotNull()) & (F.col("p.ms_played").between(112786, 225733)), "left_semi")
)

joined = (
    filtered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.source",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.country").alias("listener_country"),
    )
)

result = (
    joined
    .select(
        "id",
        F.col("ms_played").alias("ms_played_v"),
        F.col("listener_birth_year").alias("m_listener_birth_year"),
    )
    .unpivot(
        ids=["id"],
        values=["ms_played_v", "m_listener_birth_year"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
