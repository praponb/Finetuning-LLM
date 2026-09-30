from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    islands
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "burst_id",
        F.when(F.col("device_code").rlike("^(DV-02|DV-03)$"), "GROUP_A").when(F.col("device_code").rlike("^(DV-01|DV-04)$"), "EXTENDED").otherwise("GROUP_B").alias("device_code_zone"),
    )
    .orderBy(F.col("reading_ts").desc_nulls_last(), F.col("device_code_zone").desc())
)
