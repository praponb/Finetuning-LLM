from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-09-23").cast("date")) & F.col("browser").isNotNull())
    .select("*", F.to_date("started_at").alias("started_at_date"))
)

with_prev = valid.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

not_flagged = (
    sessionized
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_anti")
)

with_dims = (
    not_flagged
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.city"), F.col("p.segment"), F.col("p.signup_date"))
)

result = (
    with_dims
    .groupBy("city")
    .pivot("browser", ["Safari", "Edge"])
    .agg(F.avg("session_start_flag"))
    .select("city", F.col("Safari").alias("safari"), F.col("Edge").alias("edge"))
)
