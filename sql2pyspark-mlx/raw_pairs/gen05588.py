from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "session_start_flag",
        "session_id",
        F.when(F.col("device_code").rlike("^(DV-01|DV-04)$"), "GROUP_A").otherwise("CORE").alias("device_code_zone"),
    )
    .orderBy("firmware")
)
