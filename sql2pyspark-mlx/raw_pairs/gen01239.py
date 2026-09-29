from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy(F.year("reading_ts"), "device_code").orderBy("status_bits", "id")
w2 = Window.partitionBy(F.date_trunc("week", "reading_ts"), "device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_device_readings
    .select(
        "*",
        F.lead("pressure", 2).over(w_hist).alias("next_pressure"),
        F.sum("pressure").over(w2).alias("cumulative_pressure"),
    )
)
