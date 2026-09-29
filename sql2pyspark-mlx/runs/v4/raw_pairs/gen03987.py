from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("started_at") > F.to_date(F.lit("2023-11-16")))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.concat("device", F.lit(" "), "browser").alias("device_browser"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_clean"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.page").isin(["/search", "/checkout", "/home"])), "left_semi")
)

gaps = (
    kept
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessions_sessions
    .groupBy("burst_id")
    .agg(
        F.sum(F.when(F.col("device") == "tablet", F.col("is_new_session"))).alias("tablet_total_is_new_session"),
        F.sum(F.when(F.col("device") == "desktop", F.col("burst_id")).otherwise(0)).alias("desktop_burst_id"),
    )
    .orderBy(F.col("burst_id").desc_nulls_last())
)
