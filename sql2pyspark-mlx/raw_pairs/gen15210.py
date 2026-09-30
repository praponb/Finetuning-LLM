from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("reading_ts")).orderBy("pressure", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_device_readings
    .select(
        "id",
        "humidity",
        "status_bits",
        F.lead("status_bits", 1).over(w).alias("next_status_bits"),
        F.row_number().over(w2).alias("row_seq"),
        F.sum("temp_c").over(w3).alias("running_temp_c"),
    )
)
