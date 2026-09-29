from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("page").orderBy(F.col("page_length").desc_nulls_first(), "id")
w3 = Window.partitionBy("session_id").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-03-30 03:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.length("page").alias("page_length"),
        F.substring("page", 1, 2).alias("page_prefix"),
    )
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "page_prefix",
        "rn",
        F.sum("page_length").over(w3).alias("running_sum_page_length"),
        F.avg("page_length").over(w3).alias("running_avg_page_length"),
        F.min("page_length").over(w3).alias("running_min_page_length"),
        F.count("page_length").over(w3).alias("running_count_page_length"),
    )
)

totals = (
    with_windows
    .groupBy(
        F.when(F.col("running_count_page_length") >= 13, "LARGE").otherwise("SMALL").alias("running_count_page_length_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("running_avg_page_length"), 2).alias("avg_running_avg_page_length"),
        F.round(F.avg("running_count_page_length"), 2).alias("avg_running_count_page_length"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)

result = (
    totals
    .select(
        "running_count_page_length_tier",
        "distinct_ids",
        "num_rows",
        "avg_running_count_page_length",
        "distinct_ids_2",
    )
    .orderBy(F.col("distinct_ids_2").asc_nulls_last())
)
