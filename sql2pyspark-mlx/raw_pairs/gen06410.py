from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("activity_month").orderBy(F.col("safari_max_session_no").desc())

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-07-05 11:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        F.initcap(F.trim("country")).alias("country_title"),
    )
)

prev_events = filtered.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.date_trunc("MONTH", "prev_started_at").alias("activity_month"),
        F.when(F.col("session_no") >= 7, "HIGH").otherwise("LOW").alias("session_no_tier"),
    )
    .agg(
        F.max(F.when(F.col("browser") == "Safari", F.col("session_no"))).alias("safari_max_session_no"),
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_count"),
        ((F.unix_timestamp(F.max("prev_started_at")) - F.unix_timestamp(F.min("prev_started_at"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("safari_max_session_no") < 1000000)
    .withColumn("safari_max_session_no_rank", F.rank().over(w3))
    .orderBy(F.col("span_minutes").desc_nulls_last())
)
