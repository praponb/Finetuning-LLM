from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "browser",
        "started_at",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

result = (
    islands
    .groupingSets([["burst_id", "browser"], ["burst_id"], []], "burst_id", "browser")
    .agg(
        F.max("started_at").alias("last_seen"),
        F.sum("burst_id").alias("total_burst_id"),
    )
)
