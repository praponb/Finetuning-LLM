from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("country").isNotNull() & (~F.col("browser").eqNullSafe(F.lit("Firefox"))))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.concat("device", F.lit(" "), "browser").alias("device_browser"),
    )
)

gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "customer_id",
        "started_at",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.min("started_at").alias("started_at"),
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id_2"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("avg_island_id_2") >= 0)
)
