from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("device") == "mobile")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.hour("started_at").alias("started_at_hour"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .rollup("customer_id", "device", "burst_id")
    .agg(
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_count"),
        F.max("started_at").alias("last_seen"),
        F.sum(F.when(F.col("browser") == "Chrome", F.col("is_new_burst")).otherwise(0)).alias("chrome_is_new_burst"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("chrome_is_new_burst") > 0)
    .orderBy(F.col("device").asc_nulls_first())
)
