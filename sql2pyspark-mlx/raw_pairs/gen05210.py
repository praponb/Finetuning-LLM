from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .cube(
        "event_type",
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
        F.when(F.col("duration_sec") >= 374, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.sum(F.when(F.col("page") == "/checkout", F.col("island_id"))).alias("checkout_total_island_id"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
    .filter(F.col("checkout_total_island_id") > 0)
)
