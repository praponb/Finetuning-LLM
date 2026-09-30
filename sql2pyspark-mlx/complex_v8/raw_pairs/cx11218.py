from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage = Window.orderBy("voltage", "id")
w_quality = Window.partitionBy("quality")
w_quality_site_id_by_voltage = Window.partitionBy("quality", "site_id").orderBy("voltage", "id")

base = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

with_trends = (
    base
    .select(
        "id",
        "quality",
        F.lead("voltage").over(w_all_by_voltage).alias("next_voltage"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.row_number().over(w_quality_site_id_by_voltage).alias("voltage_seq"),
    )
)

first_rows = (
    with_trends
    .filter((F.col("voltage_seq") <= 3) & (F.col("rows_in_quality") >= 9))
    .drop("voltage_seq")
)

result = (
    first_rows
    .select(
        "id",
        "quality",
        "next_voltage",
        "rows_in_quality",
    )
    .orderBy("quality", "id")
    .limit(20)
)
