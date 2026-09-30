from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("burst_id").orderBy(F.col("burst_id_p95_2").desc())

filtered = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.length("device").alias("device_length"),
        F.coalesce("browser", F.lit("OTHER")).alias("resolved_browser"),
    )
)

with_gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "resolved_browser",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    islands
    .groupBy("burst_id", "customer_id")
    .agg(
        F.percentile_approx("burst_id", 0.95).alias("burst_id_p95"),
        F.percentile_approx("burst_id", 0.5).alias("burst_id_p50"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_rows"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.percentile_approx("burst_id", 0.95).alias("burst_id_p95_2"),
    )
    .withColumn("burst_id_p95_2_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("desktop_rows").desc_nulls_first())
)
