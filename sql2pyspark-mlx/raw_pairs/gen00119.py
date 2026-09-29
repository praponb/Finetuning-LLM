from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy("start_date_period", "id")
w3 = Window.partitionBy("status").orderBy("start_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("session_id").desc())
w5 = Window.partitionBy("status").orderBy(F.col("session_id_rank").desc(), "id")
w6 = Window.partitionBy("status").orderBy(F.col("session_id_rank").desc_nulls_last(), "id")

filtered = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-04-17").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "start_date",
        "status",
        F.date_trunc("month", "start_date").alias("start_date_period"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

prev_events = deduped.withColumn("prev_start_date_period", F.lag("start_date_period").over(w2))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_start_date_period").isNull() | (F.col("start_date_period") > F.col("prev_start_date_period") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "start_date",
        "status",
        "start_date_period",
        F.sum("is_new_burst").over(w3).alias("session_id"),
    )
)

scored = (
    islands
    .withColumn("running_session_id", F.sum("session_id").over(w3))
    .withColumn("session_id_rank", F.rank().over(w4))
)

deduped_subscriptions = (
    scored
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

deduped_subscriptions_2 = (
    deduped_subscriptions
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

result = (
    deduped_subscriptions_2
    .groupBy(
        F.when(F.col("session_id") >= 8, "LARGE").otherwise("SMALL").alias("session_id_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_rows"),
        F.min("start_date_period").alias("first_seen"),
        F.percentile_approx("session_id_rank", [0.25, 0.5, 0.75]).alias("session_id_rank_quartiles"),
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_rows"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("paused_rows") > 1)
    .orderBy("paused_rows")
)
