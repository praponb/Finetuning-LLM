from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("island_id") >= 6, "PREMIUM").otherwise("STANDARD").alias("island_id_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("island_id", 0.9).alias("island_id_p90"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.min("started_at").alias("first_started_at"),
        F.percentile_approx("island_id", 0.9).alias("island_id_p90_2"),
        F.min("started_at").alias("started_at"),
    )
)
