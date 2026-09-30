from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("device").orderBy("started_at", "id")
w3 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device").orderBy("duration_sec", "id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("duration_sec").desc(), "id")

base = (
    df_sessions
    .filter((F.col("started_at") > F.to_date(F.lit("2023-04-28"))) & (F.col("started_at") >= "2025-01-01"))
    .withColumn("browser_length", F.length("browser"))
    .withColumn("resolved_country", F.coalesce("country", F.lit("OTHER")))
)

with_latest = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"))
)

with_gaps = (
    with_latest
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "browser_length",
        "duration_sec",
        F.sum("is_new_burst").over(w3).alias("session_no"),
    )
)

scored = (
    sessionized
    .select(
        "*",
        F.lag("duration_sec").over(w4).alias("prev_duration_sec"),
        F.lag("browser_length").over(w4).alias("prev_browser_length"),
        F.row_number().over(w4).alias("seq_no"),
        F.ntile(3).over(w5).alias("duration_sec_bucket"),
    )
)

result = (
    scored
    .cube("seq_no")
    .agg(
        F.sum("prev_browser_length").alias("total_prev_browser_length"),
        F.sum(F.when(F.col("device") == "mobile", F.col("duration_sec")).otherwise(0)).alias("mobile_duration_sec"),
        F.min("started_at").alias("first_started_at"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
    .filter(F.col("mobile_duration_sec") > 0)
)
