from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc_nulls_last())

metrics = (
    df_device_readings
    .filter(F.col("reading_ts") < "2023-05-07")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        F.first("pressure", ignorenulls=True).over(w).alias("first_known_pressure"),
        F.cume_dist().over(w2).alias("temp_c_cume_dist"),
    )
)

result = (
    metrics
    .select("id", "device_code", "temp_c_cume_dist")
    .orderBy(F.col("temp_c_cume_dist").asc_nulls_last())
)
