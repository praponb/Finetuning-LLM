from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & (~F.col("page").eqNullSafe(F.lit("/cart"))))
    .withColumn("event_type_upper", F.upper("event_type"))
    .withColumn("page_group", F.when(F.col("page").isin(["/cart", "/search"]), "Group A").otherwise("Other"))
)

prev_events = base.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "page_group",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .cube(
        "event_type",
        F.date_trunc("WEEK", "event_time").alias("week_start"),
        F.when(F.col("duration_sec") >= 398, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
    )
    .orderBy(F.col("num_event_types").desc_nulls_last())
)
