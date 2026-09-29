from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_last(), "id")

latest = (
    df_web_events
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "duration_sec")
)

result = (
    latest
    .groupBy(
        F.when(F.col("duration_sec") >= 506, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.percentile_approx("duration_sec", 0.5).alias("duration_sec_p50"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
    )
)
