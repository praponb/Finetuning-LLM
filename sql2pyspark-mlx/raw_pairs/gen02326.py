from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("event_time", "id")

scored = (
    df_web_events
    .select(
        "*",
        F.avg("duration_sec").over(w).alias("page_avg_duration_sec"),
        F.sum("duration_sec").over(w2).alias("running_duration_sec"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("page_avg_duration_sec") >= 465, "HIGH").otherwise("LOW").alias("page_avg_duration_sec_tier"),
    )
    .agg(
        F.size(F.collect_set("page")).alias("num_pages"),
        F.countDistinct("page").alias("distinct_pages"),
        F.size(F.collect_set("page")).alias("num_pages_2"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
    )
    .orderBy(F.col("num_pages_2").asc_nulls_last())
)
