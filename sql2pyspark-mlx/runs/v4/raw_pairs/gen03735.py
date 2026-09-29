from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy(F.col("event_time").desc(), "id")
w3 = Window.partitionBy("page").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("is_cart", F.when(F.col("page") == "/cart", 1).otherwise(0))
    .withColumn("page_event_type", F.concat("page", F.lit(" - "), "event_type"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.avg("is_cart").over(w).alias("is_cart_avg_2"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

enriched = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.country"))
)

valid = (
    enriched
    .filter(F.col("duration_sec") == 405)
    .select(
        "*",
        F.when(F.col("duration_sec") >= 462, "High").when(F.col("duration_sec") >= 206, "Medium").otherwise("Low").alias("duration_sec_band"),
        F.initcap(F.trim("event_type")).alias("event_type_title"),
    )
)

enriched_web_events = (
    valid
    .select(
        "*",
        F.first("duration_sec", ignorenulls=True).over(w3).alias("first_known_duration_sec"),
        F.avg("duration_sec").over(w4).alias("duration_sec_avg_7d"),
    )
)

with_dims = (
    enriched_web_events
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "inner")
    .select(
        "src.*",
        F.col("b.browser"),
        F.col("b.device"),
        F.col("b.country").alias("session_country"),
    )
)

joined = (
    with_dims
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.device").alias("session_device"),
        F.col("d.country").alias("session_country_2"),
    )
)

result = (
    joined
    .groupBy(
        F.date_trunc("WEEK", "event_time").alias("week_start"),
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
    )
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.count(F.when(F.col("session_country") == "DE", 1)).alias("de_count"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
    )
)
