from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.round(F.sum("is_new_session") / F.nullif(F.count(F.when(F.col("page") == "/checkout", 1)), F.lit(0)), 2).alias("is_new_session_per_checkout"),
        F.percentile_approx("island_id", 0.5).alias("island_id_p50"),
    )
    .orderBy(F.col("island_id_p50").asc_nulls_last())
)
