from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id")

enriched = (
    df_device_readings
    .withColumn("temp_c_share", F.round((F.col("temp_c") / F.sum("temp_c").over(w)), 4))
    .withColumn("temp_c_bucket", F.ntile(5).over(w2))
)

result = (
    enriched
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "temp_c_share",
        "temp_c_bucket",
        F.when(F.col("device_code").rlike("^(DV-01|DV-04)$"), "EXTENDED").when(F.col("device_code").rlike("^(DV-02|DV-03)$"), "GROUP_B").otherwise("CORE").alias("device_code_zone"),
    )
)
