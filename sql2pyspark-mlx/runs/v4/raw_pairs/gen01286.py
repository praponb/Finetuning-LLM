from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time_quarter").desc(), "id")
w2 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("page", "session_id").orderBy("event_time_quarter", "id")

valid = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/home", "/checkout"]))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.quarter("event_time").alias("event_time_quarter"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    deduped
    .select(
        "*",
        F.max("duration_sec").over(w2).alias("duration_sec_max_7d"),
        F.sum("duration_sec").over(w3).alias("running_duration_sec"),
        F.last("page", ignorenulls=True).over(w4).alias("last_known_page"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("event_seq") >= 6, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
    )
    .agg(
        F.max("running_duration_sec").alias("max_running_duration_sec"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.count("*").alias("event_count"),
        F.countDistinct("page").alias("distinct_pages"),
        F.percentile_approx("event_seq", 0.9).alias("event_seq_p90"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
    )
)
