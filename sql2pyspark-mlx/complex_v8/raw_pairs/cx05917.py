from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")

recent_readings = (
    df_meter_readings
    .filter(F.col("voltage").between(214.3, 236.7))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

ranked = (
    recent_readings
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        F.min("voltage").over(w_site_id).alias("site_id_min_voltage"),
        F.ntile(5).over(w_all_by_voltage).alias("voltage_quintile"),
    )
)

result = (
    ranked
    .filter(F.col("quality") != "EST")
    .select(
        "id",
        "quality",
        "voltage_quintile",
        F.least(F.col("kwh"), F.col("site_id_min_voltage")).alias("least_kwh_site_id_min_voltage"),
    )
    .orderBy("quality", F.col("id").desc_nulls_first())
)
