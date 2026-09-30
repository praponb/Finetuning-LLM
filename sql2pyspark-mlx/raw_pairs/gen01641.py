from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w4 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-02-23").cast("date")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.to_date("started_at").alias("started_at_date"),
        F.lower("device").alias("device_lower"),
        F.coalesce("country", F.lit("N/A")).alias("country_clean"),
    )
)

prev_events = base.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

with_gaps = (
    sessions_sessions
    .withColumn("new_group_flag_2", F.when(F.col("started_at") - F.lag("started_at").over(w3) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_sessions_2 = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "started_at_date",
        "device_lower",
        "country_clean",
        F.sum("new_group_flag_2").over(w4).alias("island_id"),
    )
)

joined = (
    sessions_sessions_2
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.device",
        "src.started_at_date",
        "src.country_clean",
        "src.island_id",
        F.col("d.age"),
        F.col("d.country"),
    )
)

result = (
    joined
    .groupBy("country")
    .agg(
        F.min("age").alias("min_age"),
        F.sum("island_id").alias("total_island_id"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
)
