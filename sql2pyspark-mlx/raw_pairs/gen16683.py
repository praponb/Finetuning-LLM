from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-08-24 08:00:00").cast("timestamp")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.col("started_at").cast("date").alias("started_at_day"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("resolved_browser"),
    )
)

flagged = (
    base
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_sessions
    .cube(
        F.when(F.col("is_new_burst") >= 1, "LARGE").otherwise("SMALL").alias("is_new_burst_tier"),
    )
    .agg(
        F.max("burst_id").alias("max_burst_id"),
        F.min("started_at").alias("first_started_at"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.percentile_approx("is_new_burst", [0.25, 0.5, 0.75]).alias("is_new_burst_quartiles"),
        F.percentile_approx("burst_id", 0.95).alias("burst_id_p95"),
    )
)
