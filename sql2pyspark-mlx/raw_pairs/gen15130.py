from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.concat("browser", F.lit("/"), "device").alias("browser_device"))
)

gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_sessions
    .withColumn("week_start", F.date_trunc("WEEK", "started_at"))
    .withColumn("activity_month", F.date_trunc("MONTH", "started_at"))
    .groupingSets(
        [["week_start", "device", "activity_month"], ["week_start", "device"], ["week_start"]],
        "week_start",
        "device",
        "activity_month",
    )
    .agg(
        F.sum(F.when(F.col("browser") == "Safari", F.col("session_id")).otherwise(0)).alias("safari_session_id"),
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("browser") == "Safari", 1)), F.lit(0)), 2).alias("session_start_flag_per_safari"),
        F.max("session_start_flag").alias("max_session_start_flag"),
        F.grouping("week_start").alias("gid"),
    )
)
