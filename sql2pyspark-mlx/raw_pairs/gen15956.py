from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .groupBy("burst_id")
    .agg(
        F.count_if(F.col("browser") == "Firefox").alias("firefox_events"),
        F.min("started_at").alias("first_started_at"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_rows"),
    )
    .filter(F.col("firefox_events") > 1)
    .orderBy(F.col("firefox_events").asc_nulls_first())
)
