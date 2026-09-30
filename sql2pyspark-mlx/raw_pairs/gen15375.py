from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("reading_ts"), "device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("pressure", "id")

result = (
    df_device_readings
    .filter(F.col("pressure") < 1013)
    .select(
        "id",
        "humidity",
        "pressure",
        "status_bits",
        F.lag("humidity", 1).over(w).alias("prev_humidity"),
        F.lead("humidity", 1).over(w2).alias("next_humidity"),
    )
)
