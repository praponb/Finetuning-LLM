from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")

cleaned = (
    df_device_readings
    .filter(F.hour("reading_ts").between(8, 13))
    .withColumn("days_since_reading_ts", F.datediff(F.current_date(), "reading_ts"))
)

result = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
