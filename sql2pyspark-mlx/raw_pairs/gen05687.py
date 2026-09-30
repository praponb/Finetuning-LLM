from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("event_time")).orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id")

with_windows = (
    df_web_events
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        F.sum("duration_sec").over(w).alias("cumulative_duration_sec"),
        F.ntile(3).over(w2).alias("duration_sec_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("duration_sec") >= 511, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
    )
    .filter(F.col("checkout_count") > 1)
    .orderBy(F.col("duration_sec_tier").asc_nulls_first())
)
