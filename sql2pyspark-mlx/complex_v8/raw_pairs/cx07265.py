from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_by_energy_cost = Window.partitionBy(F.year("read_at")).orderBy("energy_cost")
w_read_at_year_by_energy_cost_2 = Window.partitionBy(F.year("read_at")).orderBy("energy_cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

joined_ranges = (
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
    joined_ranges
    .select(
        "*",
        F.round(F.cume_dist().over(w_read_at_year_by_energy_cost), 3).alias("energy_cost_cume_dist"),
        F.rank().over(w_read_at_year_by_energy_cost).alias("energy_cost_rank"),
        F.last("kwh").over(w_read_at_year_by_energy_cost_2).alias("last_kwh"),
    )
)

leaders = (
    sequenced
    .filter((F.col("energy_cost_rank") <= 2) & (F.col("energy_cost_cume_dist").between(0.2, 0.7)))
    .drop("energy_cost_rank")
)

result = (
    leaders
    .cube("quality", "zone", "site_id")
    .agg(
        F.round(F.avg("last_kwh"), 2).alias("avg_last_kwh"),
        F.sum(F.when(F.col("zone") == "Z2", 1).otherwise(0)).alias("n_zone_z2"),
    )
)
