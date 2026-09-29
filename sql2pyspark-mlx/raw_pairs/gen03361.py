from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-07-13"))) & F.col("country").isNotNull())
    .withColumn("browser_upper", F.upper("browser"))
)

flagged = (
    filtered
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "device",
        "started_at",
        "country",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

result = (
    islands
    .groupBy("device")
    .agg(
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("device") == "tablet", 1)), F.lit(0)), 2).alias("session_id_per_tablet"),
        F.max(F.when(F.col("device") == "desktop", F.col("session_id"))).alias("desktop_max_session_id"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
    )
    .filter(F.col("session_id_per_tablet") < 1000000)
    .orderBy(F.col("device").desc())
)
