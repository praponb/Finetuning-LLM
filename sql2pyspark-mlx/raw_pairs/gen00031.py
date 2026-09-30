from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .withColumn("island_id_tier", F.when(F.col("island_id") >= 7, "PREMIUM").otherwise("STANDARD"))
    .withColumn("year_start", F.date_trunc("YEAR", "event_time"))
    .groupingSets(
        [["island_id_tier", "session_id", "year_start"], ["island_id_tier", "session_id"], ["island_id_tier"], []],
        "island_id_tier",
        "session_id",
        "year_start",
    )
    .agg(
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count_2"),
        F.min("event_time").alias("first_seen"),
        F.count_if(F.col("page") == "/product").alias("product_events"),
        F.percentile_approx("session_start_flag", 0.95).alias("session_start_flag_p95"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.grouping_id().alias("grouping_id"),
    )
)
