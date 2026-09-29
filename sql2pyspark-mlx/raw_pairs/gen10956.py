from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupingSets([["device", "island_id"], ["device"]], "device", "island_id")
    .agg(
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.min("island_id").alias("min_island_id"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("avg_island_id") < 1000000)
    .orderBy(F.col("device").desc_nulls_first())
)
