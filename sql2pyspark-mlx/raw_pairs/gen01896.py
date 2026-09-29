from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w4 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-09-08 08:00:00").cast("timestamp"))
    .withColumn("country_lower", F.lower("country"))
)

prev_events = filtered.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    flagged
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

flagged_2 = (
    sessions_sessions
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w3).isNull() | (F.col("started_at") > F.lag("started_at").over(w3) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = flagged_2.withColumn("island_id", F.sum("is_new_session").over(w4))

kept = (
    islands
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_anti")
)

with_dims = (
    kept
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.signup_date"), F.col("p.segment"), F.col("p.age"))
)

pivoted = (
    with_dims
    .groupBy("browser")
    .pivot("device", ["mobile", "tablet"])
    .agg(F.max("session_id").alias("total"), F.count(F.lit(1)).alias("cnt"))
)

result = (
    pivoted
    .select("browser", "mobile_total")
    .orderBy("mobile_total")
)
