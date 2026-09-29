from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2023-04-30 20:00:00"))) & F.col("customer_id").isNotNull())
    .withColumn("browser_title", F.initcap(F.trim("browser")))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("unassigned")))
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy("country", "customer_id")
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.countDistinct("browser").alias("distinct_browsers"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
    )
    .orderBy(F.col("de_events").asc_nulls_first())
)
