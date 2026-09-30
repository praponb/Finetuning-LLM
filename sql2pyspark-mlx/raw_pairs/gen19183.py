from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupingSets([["browser", "country"], ["browser"]], "browser", "country")
    .agg(
        F.max("started_at").alias("ended_at"),
        F.percentile_approx("session_id", 0.95).alias("session_id_p95"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("country") == "UK", 1)), F.lit(0)), 2).alias("session_id_per_uk"),
    )
    .orderBy(F.col("session_id_p95").desc_nulls_last())
)
