from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("reading_ts")).orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("reading_ts"), "device_code")

metrics = (
    df_device_readings
    .filter(F.col("firmware").isin(["1.0.3", "2.0.1"]))
    .withColumn("running_pressure", F.sum("pressure").over(w))
    .withColumn("corr_status_bits_pressure", F.corr("status_bits", "pressure").over(w2))
)

result = (
    metrics
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "corr_status_bits_pressure",
    )
    .orderBy("reading_ts")
)
