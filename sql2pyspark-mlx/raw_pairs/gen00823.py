from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc())
w2 = Window.partitionBy("year_start").orderBy(F.col("avg_duration_sec").desc())

deduped = (
    df_web_events
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupBy(
        F.date_trunc("YEAR", "event_time").alias("year_start"),
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
        F.when(F.col("duration_sec") >= 369, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/cart", 1)), F.lit(0)), 2).alias("duration_sec_per_cart"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_rows"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .withColumn("avg_duration_sec_rank", F.rank().over(w2))
    .orderBy("search_rows")
)
