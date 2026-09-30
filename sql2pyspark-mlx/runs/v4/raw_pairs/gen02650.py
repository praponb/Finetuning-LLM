from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        "browser",
        F.when(F.col("is_new_session") >= 1, "HIGH").otherwise("LOW").alias("is_new_session_tier"),
    )
    .agg(
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.round(F.sum("is_new_session") / F.nullif(F.count(F.when(F.col("browser") == "Safari", 1)), F.lit(0)), 2).alias("is_new_session_per_safari"),
        F.percentile_approx("is_new_session", 0.5).alias("is_new_session_p50"),
    )
    .orderBy("avg_is_new_session")
)
