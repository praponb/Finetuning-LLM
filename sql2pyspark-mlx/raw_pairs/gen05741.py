from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .select(
        "id",
        "device_code",
        "humidity",
        "status_bits",
        "firmware",
        F.when(F.col("device_code").rlike("^(DV-02|DV-01)$"), "CORE").when(F.col("device_code").rlike("^(DV-03|DV-04)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("device_code_zone"),
    )
    .orderBy(F.col("firmware").desc())
)
