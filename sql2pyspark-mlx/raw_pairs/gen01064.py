from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w2 = Window.partitionBy(F.year("reading_ts"))

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        F.ntile(5).over(w).alias("status_bits_bucket"),
        F.round((F.col("pressure") / F.sum("pressure").over(w2)), 4).alias("pressure_share"),
    )
)
