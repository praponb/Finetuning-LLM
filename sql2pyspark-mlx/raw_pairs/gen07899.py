from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")

ranked = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy(F.date_trunc("QUARTER", "event_time").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.countDistinct("page").alias("distinct_pages"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_rows"),
        F.percentile_approx("duration_sec", 0.5).alias("duration_sec_p50"),
    )
)
