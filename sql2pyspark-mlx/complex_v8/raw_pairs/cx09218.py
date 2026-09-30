from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy(F.col("voltage").desc())
w_quality_site_id_by_voltage_p = Window.partitionBy("quality", "site_id").orderBy("voltage_plus_kwh", "id").rowsBetween(-3, -1)
w_quality_read_at_year_by_volt = Window.partitionBy("quality", F.year("read_at")).orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_readings = (
    df_meter_readings
    .filter(F.col("quality").isin("BAD", "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
    )
)

with_windows = (
    base_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage_plus_kwh",
        F.sum("voltage_plus_kwh").over(w_site_id).alias("site_id_sum_voltage_plus_kwh"),
        F.round(F.col("voltage_plus_kwh") / F.nullif(F.sum("voltage_plus_kwh").over(w_site_id_quality), F.lit(0)), 4).alias("voltage_plus_kwh_share_of_site_id_quality"),
        F.round(F.cume_dist().over(w_site_id_by_voltage_ties), 3).alias("voltage_cume_dist"),
        F.round(F.avg("voltage").over(w_quality_site_id_by_voltage_p), 2).alias("voltage_avg_3_prior"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.first("voltage_plus_kwh", ignorenulls=True).over(w_quality_read_at_year_by_volt).alias("first_known_voltage_plus_kwh"),
        F.round(F.col("voltage_plus_kwh") / F.nullif(F.sum("voltage_plus_kwh").over(w_site_id), F.lit(0)), 4).alias("voltage_plus_kwh_share_of_site_id"),
        F.count(F.lit(1)).over(w_site_id_quality).alias("rows_in_site_id_quality"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "kwh",
        "site_id_sum_voltage_plus_kwh",
        "voltage_plus_kwh_share_of_site_id_quality",
        "voltage_avg_3_prior",
        "distinct_quality_in_site_id",
        "voltage_plus_kwh_share_of_site_id",
        "rows_in_site_id_quality",
    )
    .orderBy(F.col("distinct_quality_in_site_id").desc(), F.col("id").desc())
)
