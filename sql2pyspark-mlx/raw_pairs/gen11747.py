from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device_code").orderBy("reading_ts", "id")

base = (
    df_device_readings
    .filter(F.to_date("reading_ts") < "2024-05-01")
    .select("*", F.round("humidity", 1).alias("humidity_rounded"))
)

result = (
    base
    .filter(F.col("firmware") == "1.0.3")
    .select(
        "*",
        F.lag("humidity").over(w_grp).alias("prev_humidity"),
        F.lead("pressure").over(w_grp).alias("next_pressure"),
        F.lag("pressure").over(w_grp).alias("prev_pressure"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)
