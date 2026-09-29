from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("started_at", "id")
w4 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2023-06-21 09:00:00"))) & (F.to_date("started_at") >= "2023-11-05"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.concat("device", F.lit(" "), "browser").alias("device_browser"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("resolved_country"),
    )
)

gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

with_dims = (
    islands
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.credit_limit"))
)

with_prev = with_dims.withColumn("prev_started_at", F.lag("started_at").over(w3))

gaps_2 = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps_2.withColumn("burst_id", F.sum("session_start_flag").over(w4))

summary = (
    sessionized
    .groupBy(
        "customer_id",
        F.when(F.col("burst_id") >= 7, "HIGH").otherwise("LOW").alias("burst_id_tier"),
        F.when(F.col("session_no") >= 6, "PREMIUM").otherwise("STANDARD").alias("session_no_tier"),
    )
    .agg(
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.avg("is_new_burst").alias("avg_is_new_burst"),
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
    )
)

result = (
    summary
    .select(
        "customer_id",
        "burst_id_tier",
        "session_no_tier",
        "num_browsers",
        "mobile_events",
    )
)
