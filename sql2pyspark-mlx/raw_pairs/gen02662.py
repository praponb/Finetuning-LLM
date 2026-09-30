from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_no_tier").orderBy(F.col("avg_pressure").desc())

with_gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("session_no") >= 6, "LARGE").otherwise("SMALL").alias("session_no_tier"),
        F.when(F.col("pressure") >= 1018, "HIGH").otherwise("LOW").alias("pressure_tier"),
    )
    .agg(
        F.round(F.avg("status_bits"), 2).alias("avg_status_bits"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("avg_pressure_rank", F.rank().over(w3))
    .orderBy(F.col("pressure_tier").desc())
)
