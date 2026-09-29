from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")

result = (
    df_device_readings
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
