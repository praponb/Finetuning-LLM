from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy("device")
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.sum(F.when(F.col("device") == "tablet", F.col("new_group_flag")).otherwise(0)).alias("tablet_new_group_flag"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("device") == "desktop", 1)), F.lit(0)), 2).alias("session_id_per_desktop"),
    )
    .orderBy(F.col("tablet_new_group_flag").desc_nulls_first())
)
