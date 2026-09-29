from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("started_at", "id")
w_grp = Window.partitionBy("browser", "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w5 = Window.partitionBy("browser").orderBy("event_seq", "id")
w_hist = Window.partitionBy("browser", "country").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("started_at") > F.to_date(F.lit("2023-03-24")))
    .withColumn("browser_device", F.concat("browser", F.lit("/"), "device"))
    .withColumn("country_prefix", F.substring("country", 1, 4))
)

with_windows = (
    valid
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w_running))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("last_known_country_2", F.last("country", ignorenulls=True).over(w_grp))
    .withColumn("seq_no", F.row_number().over(w4))
)

metrics = (
    with_windows
    .filter(F.col("event_seq") > 8)
    .select(
        "*",
        F.lag("event_seq", 1).over(w5).alias("prev_event_seq"),
        F.sum("seq_no").over(w_hist).alias("running_seq_no"),
    )
)

base = (
    metrics
    .filter(F.col("country").isNotNull() & (F.col("started_at") > F.to_date(F.lit("2023-04-30"))))
    .withColumn("seq_no_whole", F.ceil("seq_no"))
)

result = (
    base
    .groupBy("running_seq_no")
    .agg(
        F.avg("seq_no_whole").alias("avg_seq_no_whole"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.percentile_approx("seq_no_whole", 0.9).alias("seq_no_whole_p90"),
    )
)
