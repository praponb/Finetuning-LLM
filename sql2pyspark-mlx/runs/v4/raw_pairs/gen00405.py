from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_web_events
    .rollup("session_id")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("new_group_flag") / F.nullif(F.count(F.when(F.col("page") == "/checkout", 1)), F.lit(0)), 2).alias("new_group_flag_per_checkout"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("session_id").desc_nulls_last())
)
