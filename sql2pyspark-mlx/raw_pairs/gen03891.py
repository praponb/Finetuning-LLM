from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")
w2 = Window.partitionBy(F.month("event_time"), "event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-08-04").cast("date"))
    .withColumn("page_filled", F.coalesce("page", F.lit("N/A")))
    .withColumn("page_prefix", F.substring("page", 1, 3))
    .withColumn("page_clean", F.coalesce("page", F.lit("OTHER")))
)

result = (
    cleaned
    .select(
        "id",
        "duration_sec",
        "page_prefix",
        "page_clean",
        F.stddev_samp("duration_sec").over(w).alias("event_type_stddev_samp_duration_sec"),
        F.sum("duration_sec").over(w2).alias("duration_sec_sum_90d"),
    )
)
