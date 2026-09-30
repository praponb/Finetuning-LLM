from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

result = (
    islands
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("device") == "desktop", F.col("session_id"))).alias("desktop_total_session_id"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sort_array(F.collect_set("country")).alias("country_list_2"),
    )
)
