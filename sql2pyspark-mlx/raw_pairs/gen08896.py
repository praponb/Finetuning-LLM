from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-08-14 09:00:00").cast("timestamp")) & F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.initcap(F.trim("country")).alias("country_title"),
        F.coalesce("country", F.lit("OTHER")).alias("resolved_country"),
    )
)

flagged = (
    base
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy("browser")
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.percentile_approx("session_no", 0.95).alias("session_no_p95"),
        F.sum(F.when(F.col("country") == "UK", F.col("session_start_flag")).otherwise(0)).alias("uk_session_start_flag"),
    )
    .filter(F.col("th_count") > 1)
    .orderBy(F.col("th_count").desc_nulls_first())
)
