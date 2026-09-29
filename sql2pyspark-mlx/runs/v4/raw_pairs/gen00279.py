from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.countDistinct("browser").alias("distinct_browsers"),
    )
)
