from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .withColumn("quarter_start", F.date_trunc("QUARTER", "started_at"))
    .rollup("browser", "quarter_start")
    .agg(
        F.count_if(F.col("browser") == "Firefox").alias("firefox_events"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.sum(F.when(F.col("device") == "desktop", F.col("new_group_flag")).otherwise(0)).alias("desktop_new_group_flag"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        F.avg("new_group_flag").alias("avg_new_group_flag"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("firefox_events") < 1000)
)
