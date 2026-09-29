from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id")
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id")

result = (
    df_device_readings
    .withColumn("corr_temp_c_pressure", F.corr("temp_c", "pressure").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("temp_c_bucket", F.ntile(10).over(w3))
    .withColumn("seq_no", F.row_number().over(w2))
)
