from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter(F.col("source") == "search")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.abs(F.col("ms_played") - 107912).alias("ms_played_distance"),
        F.when(F.col("ms_played") >= 239955, "HIGH").when(F.col("ms_played") >= 98052, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
        (F.col("played_at") + F.expr("INTERVAL 7 DAYS")).alias("played_at_plus_7d"),
    )
)

summary = (
    filtered_plays
    .groupBy(F.date_format("played_at", "yyyy-MM").alias("played_at_ym"))
    .agg(
        F.count("*").alias("n_rows"),
        F.round(100 * F.count(F.when(F.col("source").isNotNull(), 1)) / F.count("*"), 1).alias("pct_source_is_not_null"),
    )
)

annotated = (
    summary
    .withColumn("n_rows_tier", F.when(F.col("n_rows") >= 10, "A").when(F.col("n_rows") >= 5, "B").otherwise("C"))
)

result = (
    annotated
    .select("played_at_ym", "n_rows", "pct_source_is_not_null")
    .orderBy("pct_source_is_not_null")
)
