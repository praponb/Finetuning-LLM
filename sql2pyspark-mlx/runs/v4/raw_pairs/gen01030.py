from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.to_date("event_time") < "2025-04-21")
    .withColumn("event_time_label", F.date_format("event_time", "MMM yyyy"))
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_web_events
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 465, "LARGE").otherwise("SMALL"))
    .withColumn("island_id_tier", F.when(F.col("island_id") >= 9, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["duration_sec_tier", "island_id_tier"], ["duration_sec_tier"]],
        "duration_sec_tier",
        "island_id_tier",
    )
    .agg(
        F.sum(F.when(F.col("page") == "/cart", F.col("is_new_burst")).otherwise(0)).alias("cart_is_new_burst"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
        F.max("event_time").alias("last_seen"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count_2"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/product", 1)), F.lit(0)), 2).alias("duration_sec_per_product"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"),
    )
    .orderBy(F.col("duration_sec_per_product").desc_nulls_last())
)
