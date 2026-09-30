from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        "session_id",
        F.when(F.col("device_code").rlike("^(DV-03|DV-04)$"), "PRIORITY").when(F.col("device_code").rlike("^(DV-01|DV-02)$"), "GROUP_B").otherwise("GROUP_A").alias("device_code_zone"),
    )
    .orderBy(F.col("pressure").desc_nulls_first())
)
