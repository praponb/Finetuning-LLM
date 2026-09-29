from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_site_id = Window.partitionBy("zone", "site_id")
w_all_by_kwh_full = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy(F.col("voltage").asc_nulls_last())

joined_ranges = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "left",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

sequenced = (
    joined_ranges
    .select(
        "*",
        F.round(F.avg("kwh").over(w_zone_site_id), 2).alias("zone_site_id_avg_kwh"),
        F.nth_value("kwh", 2).over(w_all_by_kwh_full).alias("second_kwh"),
        F.rank().over(w_all_by_kwh).alias("kwh_rank"),
        F.round(F.percent_rank().over(w_site_id_by_voltage_ties), 3).alias("voltage_percent_rank"),
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_zone_site_id), F.lit(0)), 4).alias("rate_share_of_zone_site_id"),
    )
)

result = (
    sequenced
    .select("id", F.col("rate").alias("m_rate"), F.col("voltage").alias("voltage_v"))
    .unpivot(
        ids=["id"],
        values=["m_rate", "voltage_v"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
