from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("browser").isNotNull() & (~F.col("country").eqNullSafe(F.lit("DE"))))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.date_format("started_at", "MMM yyyy").alias("started_at_label"),
        F.col("started_at").cast("date").alias("started_at_day"),
    )
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_sessions
    .groupBy("country")
    .agg(
        F.count("*").alias("event_count"),
        F.avg(F.when(F.col("device") == "mobile", F.col("session_id"))).alias("mobile_avg_session_id"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows"),
    )
    .filter(F.col("mobile_avg_session_id") < 1000000)
    .orderBy(F.col("event_count").asc_nulls_last())
)
