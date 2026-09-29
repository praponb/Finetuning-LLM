from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

joined = (
    sessionized
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.credit_limit"), F.col("c.segment"), F.col("c.city"))
)

with_gaps = (
    joined
    .withColumn("is_new_burst_2", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_no", F.sum("is_new_burst_2").over(w2))

daily = (
    sessions_sessions
    .groupBy(
        "device",
        F.when(F.col("session_no") >= 8, "HIGH").otherwise("LOW").alias("session_no_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.count_if(F.col("browser") == "Chrome").alias("chrome_events"),
    )
)

result = (
    daily
    .select(
        "device",
        "session_no_tier",
        "browser_list",
        "span_minutes",
        "chrome_events",
    )
)
