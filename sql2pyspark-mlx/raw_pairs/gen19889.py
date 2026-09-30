from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        "firmware",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .select("id", "reading_ts", "firmware", "session_no")
    .orderBy(F.col("id").desc_nulls_first())
)
