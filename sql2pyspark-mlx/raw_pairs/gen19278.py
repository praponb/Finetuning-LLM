from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("browser").eqNullSafe(F.lit("Chrome"))) & (F.col("started_at") > F.lit("2023-08-28 02:00:00").cast("timestamp")))
    .withColumn("device_length", F.length("device"))
    .withColumn("resolved_browser", F.coalesce("browser", F.lit("DIRECT")))
)

result = (
    base
    .filter(F.col("customer_id").isin([23, 29]))
    .select(
        "*",
        F.lag("device_length", 1).over(w).alias("prev_device_length"),
        F.first("device_length", ignorenulls=True).over(w2).alias("first_known_device_length"),
    )
)
