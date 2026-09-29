from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_energy_cost = Window.orderBy(F.col("energy_cost").desc(), "id")
w_zone_quality = Window.partitionBy("zone", "quality")
w_all_by_energy_cost_running = Window.orderBy(F.col("energy_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

in_window = (
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
    in_window
    .select(
        "id",
        "kwh",
        "quality",
        "zone",
        "rate",
        "energy_cost",
        F.lag("energy_cost").over(w_all_by_energy_cost).alias("prev_energy_cost"),
        F.count(F.lit(1)).over(w_zone_quality).alias("rows_in_zone_quality"),
        F.round(F.col("rate") - F.avg("rate").over(w_zone_quality), 2).alias("rate_vs_zone_quality_avg"),
        F.dense_rank().over(w_all_by_energy_cost).alias("energy_cost_dense_rank"),
        F.first("kwh", ignorenulls=True).over(w_all_by_energy_cost_running).alias("first_known_kwh"),
        F.round(F.col("energy_cost") - F.avg("energy_cost").over(w_zone_quality), 2).alias("energy_cost_vs_zone_quality_avg"),
    )
)

result = (
    sequenced
    .select("id", "rows_in_zone_quality", "energy_cost_dense_rank")
    .unpivot(
        ids=["id"],
        values=["rows_in_zone_quality", "energy_cost_dense_rank"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .orderBy("id", "measure")
)
