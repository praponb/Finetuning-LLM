from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .rollup(
        F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW").alias("session_start_flag_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.sum(F.when(F.col("browser") == "Chrome", F.col("session_no")).otherwise(0)).alias("chrome_session_no"),
        F.avg(F.when(F.col("country") == "UK", F.col("session_start_flag"))).alias("uk_avg_session_start_flag"),
    )
    .orderBy(F.col("session_start_flag_tier").desc_nulls_last())
)
