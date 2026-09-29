from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_by_voltage_minu = Window.partitionBy(F.year("read_at")).orderBy(F.col("voltage_minus_kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_year_by_voltage_minu_2 = Window.partitionBy(F.year("read_at")).orderBy(F.col("voltage_minus_kwh").asc_nulls_last(), "id")

filtered_readings = (
    df_meter_readings
    .filter((~F.col("quality").isin("BAD", "EST")) & (F.col("read_at") < F.lit("2025-02-18 07:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
        F.abs(F.col("voltage") - 231.7).alias("voltage_distance"),
    )
)

with_windows = (
    filtered_readings
    .select(
        "*",
        F.last("voltage", ignorenulls=True).over(w_read_at_year_by_voltage_minu).alias("last_known_voltage"),
        F.rank().over(w_read_at_year_by_voltage_minu_2).alias("voltage_minus_kwh_rank"),
        F.row_number().over(w_read_at_year_by_voltage_minu_2).alias("seq_no"),
    )
)

joined_site = (
    with_windows.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") == "Z4"), "inner")
    .select("s.*", F.col("si.tariff").alias("site_tariff"), "si.site_name")
)

labeled = (
    joined_site
    .filter(F.col("kwh") <= 79.4)
    .select(
        "*",
        F.when(F.col("site_tariff").isin("TOU", "EV"), "group_a").when(F.col("site_tariff") == "FLAT", "group_b").otherwise("other").alias("site_tariff_group"),
        F.round(F.col("last_known_voltage") * 1.1, 2).alias("last_known_voltage_adj"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage_minus_kwh_rank"), F.lit(0)), 3).alias("kwh_per_voltage_minus_kwh_rank"),
    )
)

result = (
    labeled
    .groupBy("read_at")
    .pivot("site_tariff_group", ["group_b", "group_a"])
    .agg(F.max("seq_no"))
    .orderBy("read_at")
)
