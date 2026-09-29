from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("source", "track_id").orderBy(F.col("played_at").desc(), F.col("id").desc())

prepared = (
    df_plays
    .filter(F.col("device").isin("tv", "phone", "speaker"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket_2"),
    )
)

with_activity = (
    prepared.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.to_date(F.col("p.played_at")) < F.lit("2025-03-08").cast("date")), "left_semi")
)

labeled = (
    with_activity
    .filter(F.col("listener_id").isNotNull())
    .select(
        "*",
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
        F.year(F.col("played_at")).alias("played_at_year"),
        F.concat(F.col("source"), F.lit("#"), F.col("ms_played_bucket").cast("string")).alias("source_ref"),
    )
)

latest = (
    labeled
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .groupingSets(
        [["source", "track_id", "listener_id"], ["source"]],
        "source", "track_id", "listener_id",
    )
    .agg(
        F.count(F.when(F.col("source").isNotNull(), 1)).alias("n_source_is_not_null"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
    )
    .select(
        F.coalesce(F.col("source"), F.lit("ALL")).alias("source"),
        "track_id",
        "listener_id",
        "n_source_is_not_null",
        "source_list",
    )
    .orderBy(
        F.col("source").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
    )
)
