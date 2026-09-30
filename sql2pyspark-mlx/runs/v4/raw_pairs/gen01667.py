from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.col("duration_sec").cast("string").alias("duration_sec_num"),
        F.upper("event_type").alias("event_type_upper"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_clean"),
    )
)

flagged = (
    cleaned
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .withColumn("week_start", F.date_trunc("WEEK", "event_time"))
    .withColumn("island_id_tier", F.when(F.col("island_id") >= 7, "PREMIUM").otherwise("STANDARD"))
    .rollup("week_start", "island_id_tier")
    .agg(
        F.size(F.collect_set("page")).alias("num_pages"),
        F.count_if(F.col("page") == "/cart").alias("cart_events"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_rows"),
        F.grouping("week_start").alias("gid"),
    )
    .orderBy(F.col("num_pages").desc())
)
