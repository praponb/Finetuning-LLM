from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("island_id_tier").orderBy(F.col("num_devices").desc())

gaps = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("island_id") >= 7, "PREMIUM").otherwise("STANDARD").alias("island_id_tier"),
        F.date_trunc("WEEK", "started_at").alias("week_start"),
    )
    .agg(
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.max("started_at").alias("ended_at"),
        F.avg("island_id").alias("avg_island_id"),
    )
    .withColumn("num_devices_percent_rank", F.percent_rank().over(w3))
)
