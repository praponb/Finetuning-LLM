from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("new_group_flag_tier").orderBy(F.col("tablet_rows").desc())

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-07-29").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.initcap(F.trim("browser")).alias("browser_title"),
    )
)

prev_events = filtered.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.when(F.col("new_group_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("new_group_flag_tier"),
        "country",
    )
    .agg(
        F.sum(F.when(F.col("device") == "desktop", F.col("new_group_flag")).otherwise(0)).alias("desktop_new_group_flag"),
        F.max(F.when(F.col("device") == "mobile", F.col("session_no"))).alias("mobile_max_session_no"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_rows"),
    )
    .withColumn("tablet_rows_rank", F.rank().over(w3))
)
