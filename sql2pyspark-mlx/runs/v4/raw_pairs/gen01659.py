from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("session_id").desc())

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 03:00:00").cast("timestamp"))
    .withColumn("device_code_upper", F.upper("device_code"))
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

flagged = (
    deduped
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

deduped_device_readings = (
    islands
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    deduped_device_readings
    .select(
        "id",
        "device_code",
        "humidity",
        F.col("temp_c").alias("TEMP_C"),
        "pressure",
    )
    .unpivot(["id", "device_code"], ["humidity", "TEMP_C", "pressure"], "kpi", "val")
)
