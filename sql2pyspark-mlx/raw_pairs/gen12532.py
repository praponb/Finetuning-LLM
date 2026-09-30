from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(-3, Window.currentRow)

metrics = (
    df_device_readings
    .filter(F.col("pressure").isNull())
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        F.avg("temp_c").over(w_recent).alias("last4_avg_temp_c"),
        F.min("temp_c").over(w_recent).alias("last4_min_temp_c"),
        F.count("temp_c").over(w_recent).alias("last4_count_temp_c"),
    )
)

result = (
    metrics
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        F.when(F.col("device_code").rlike("^(DV-04|DV-03)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("device_code_zone"),
    )
    .orderBy(F.col("pressure").desc_nulls_first(), "device_code_zone")
)
