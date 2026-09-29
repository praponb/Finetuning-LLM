from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("session_id").desc(), "id")
w4 = Window.partitionBy("browser").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device", "customer_id").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("device").orderBy("is_new_burst", "id")
w_part_2 = Window.orderBy("cumulative_session_id", "id")
w_grp = Window.partitionBy("customer_id").orderBy("cumulative_session_id", "id")
w_running = Window.partitionBy("customer_id", "device").orderBy("cumulative_session_id", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "*",
        F.concat("browser", F.lit(" "), "device").alias("browser_device"),
        F.coalesce("browser", F.lit("DIRECT")).alias("browser_filled"),
    )
)

flagged = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

latest = (
    islands
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = (
    latest
    .filter(F.col("is_new_burst") <= 1)
    .select(
        "id",
        "customer_id",
        "device",
        "browser_filled",
        "is_new_burst",
        F.first("browser", ignorenulls=True).over(w4).alias("first_known_browser"),
        F.sum("session_id").over(w5).alias("cumulative_session_id"),
    )
)

scored_sessions = (
    scored
    .select(
        "id",
        "is_new_burst",
        "first_known_browser",
        F.sum("is_new_burst").over(w6).alias("cumulative_is_new_burst"),
        F.lag("is_new_burst", 1).over(w_part).alias("prev_is_new_burst"),
        F.lag("is_new_burst", 1, 0).over(w_part_2).alias("prev_is_new_burst_2"),
        F.lag("is_new_burst", 2, 0).over(w_grp).alias("prev_is_new_burst_3"),
        F.avg("cumulative_session_id").over(w_running).alias("cumulative_session_id_avg_3"),
    )
)

eligible = (
    scored_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.session_id").isin([9, 24, 31, 39])), "left_anti")
)

valid = (
    eligible
    .filter(F.col("cumulative_is_new_burst") <= 3)
    .select(
        "id",
        "is_new_burst",
        "first_known_browser",
        "cumulative_is_new_burst",
        "prev_is_new_burst",
        "prev_is_new_burst_2",
        "prev_is_new_burst_3",
        "cumulative_session_id_avg_3",
        F.round((F.col("cumulative_is_new_burst") * 1.1), 2).alias("cumulative_is_new_burst_adjusted"),
        (F.col("prev_is_new_burst") / F.nullif("cumulative_is_new_burst", F.lit(0))).alias("prev_is_new_burst_per_cumulative_is_new_burst"),
    )
)

result = (
    valid
    .withColumn("prev_is_new_burst_2_tier", F.when(F.col("prev_is_new_burst_2") >= 1, "HIGH").otherwise("LOW"))
    .withColumn("cumulative_is_new_burst_adjusted_tier", F.when(F.col("cumulative_is_new_burst_adjusted") >= 63, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["prev_is_new_burst_2_tier", "cumulative_is_new_burst_adjusted_tier"], ["prev_is_new_burst_2_tier"]],
        "prev_is_new_burst_2_tier",
        "cumulative_is_new_burst_adjusted_tier",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
        F.percentile_approx("is_new_burst", 0.9).alias("is_new_burst_p90"),
        F.min("prev_is_new_burst_2").alias("min_prev_is_new_burst_2"),
        F.grouping("prev_is_new_burst_2_tier").alias("gid"),
    )
)
