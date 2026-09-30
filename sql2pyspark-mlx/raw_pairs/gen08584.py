from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-02-17 23:00:00").cast("timestamp")) & F.col("customer_id").isNull())
    .select("*", F.to_date("started_at").alias("started_at_date"))
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .withColumn("week_start", F.date_trunc("week", "started_at"))
    .withColumn("activity_month", F.date_trunc("month", "started_at"))
    .rollup("device", "week_start", "activity_month")
    .agg(
        F.percentile_approx("burst_id", 0.5).alias("burst_id_p50"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_rows"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("week_start").desc_nulls_last())
)
