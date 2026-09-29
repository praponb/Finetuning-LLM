from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("browser").isin(["Safari", "Edge"]))
    .withColumn("is_us", F.when(F.col("country") == "US", 1).otherwise(0))
    .withColumn("country_clean", F.coalesce("country", F.lit("NONE")))
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessions_sessions
    .groupBy(F.date_trunc("year", "started_at").alias("year_start"))
    .agg(
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("browser") == "Chrome", 1)), F.lit(0)), 2).alias("session_no_per_chrome"),
        F.max(F.when(F.col("browser") == "Chrome", F.col("is_new_session"))).alias("chrome_max_is_new_session"),
        F.percentile_approx("is_new_session", [0.25, 0.5, 0.75]).alias("is_new_session_quartiles"),
        F.max("is_us").alias("max_is_us"),
    )
    .filter(F.col("chrome_max_is_new_session") < 0)
    .orderBy(F.col("year_start").desc())
)
