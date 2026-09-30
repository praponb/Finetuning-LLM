from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits")

base = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        F.datediff(F.current_date(), "reading_ts").alias("days_since_reading_ts"),
        F.round((F.col("pressure") * 1.1), 2).alias("pressure_scaled"),
    )
)

result = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)
