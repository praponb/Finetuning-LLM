from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-09-10 01:00:00").cast("timestamp")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.trim("device").alias("clean_device"),
        F.hour("started_at").alias("started_at_hour"),
    )
)

gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "clean_device",
        "started_at_hour",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("started_at_hour") >= 65, "HIGH").otherwise("LOW").alias("started_at_hour_tier"),
        "customer_id",
        F.when(F.col("session_no") >= 7, "LARGE").otherwise("SMALL").alias("session_no_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.countDistinct("browser").alias("distinct_browsers"),
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
    )
    .filter(F.col("duration_min") < 1000000)
    .orderBy("distinct_browsers")
)
