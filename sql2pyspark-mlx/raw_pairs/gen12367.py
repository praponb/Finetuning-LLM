from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-08-01").cast("date")) & F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.initcap(F.trim("device")).alias("device_title"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .withColumn("session_id_tier", F.when(F.col("session_id") >= 8, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["session_id", "session_id_tier"], ["session_id"]],
        "session_id",
        "session_id_tier",
    )
    .agg(
        F.min("session_id").alias("min_session_id"),
        F.percentile_approx("is_new_burst", [0.25, 0.5, 0.75]).alias("is_new_burst_quartiles"),
        F.sum(F.when(F.col("browser") == "Firefox", F.col("is_new_burst")).otherwise(0)).alias("firefox_is_new_burst"),
        F.grouping("session_id").alias("grouping_id"),
    )
    .filter(F.col("firefox_is_new_burst") > 0)
    .orderBy("session_id")
)
