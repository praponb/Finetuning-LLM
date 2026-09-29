from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "customer_id",
        "started_at",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .rollup(
        F.when(F.col("island_id") >= 6, "PREMIUM").otherwise("STANDARD").alias("island_id_tier"),
        "customer_id",
    )
    .agg(
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") > 1)
)
