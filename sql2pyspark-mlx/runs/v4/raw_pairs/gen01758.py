from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .withColumn("new_group_flag_tier", F.when(F.col("new_group_flag") >= 1, "PREMIUM").otherwise("STANDARD"))
    .rollup("new_group_flag_tier")
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.count_if(F.col("browser") == "Firefox").alias("firefox_events"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("th_rows") >= 1)
    .orderBy(F.col("us_rows").asc_nulls_first())
)
