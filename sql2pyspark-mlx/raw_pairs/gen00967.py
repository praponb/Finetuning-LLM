from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")

ranked = (
    df_device_readings
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .select("id", "device_code", "temp_c", "pressure", "status_bits", "firmware")
    .orderBy(F.col("id").desc_nulls_last())
)
