from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_quality_zone_by_read_at_runn = Window.partitionBy("quality", "zone").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_zone_by_read_at_ties = Window.partitionBy("quality", "zone").orderBy("read_at")

matched = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "inner",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

sequenced = (
    matched
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "zone",
        "rate",
        F.round(F.avg("energy_cost").over(w_quality), 2).alias("quality_avg_energy_cost"),
        F.max("kwh").over(w_quality_zone_by_read_at_runn).alias("max_kwh_so_far"),
        F.rank().over(w_quality_zone_by_read_at_ties).alias("read_at_rank"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
    )
)

with_activity = (
    sequenced.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "OK"), "left_anti")
)

result = (
    with_activity
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "zone",
        "quality_avg_energy_cost",
        "read_at_rank",
        "rows_in_quality",
    )
    .orderBy(F.col("id").asc(), F.col("zone").asc_nulls_last())
)
