from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        F.last("browser", ignorenulls=True).over(w).alias("last_known_browser"),
    )
)

result = (
    metrics
    .withColumn("year_start", F.date_trunc("YEAR", "started_at"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "started_at"))
    .rollup("year_start", "quarter_start", "device")
    .agg(
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_rows"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("device").asc_nulls_first())
)
