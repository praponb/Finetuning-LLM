from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_read_at_to_date = Window.partitionBy("site_id", F.to_date("read_at"))
w_all_by_read_at_ties = Window.orderBy(F.col("read_at").asc_nulls_last())

covered = (
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
    covered
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "energy_cost",
        F.sum("voltage").over(w_site_id_read_at_to_date).alias("site_id_read_at_to_date_sum_voltage"),
        F.round(F.percent_rank().over(w_all_by_read_at_ties), 3).alias("read_at_percent_rank"),
    )
)

screened = (
    sequenced.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isNotNull()), "left_anti")
)

joined = (
    screened.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.energy_cost",
        "s.site_id_read_at_to_date_sum_voltage",
        "s.read_at_percent_rank",
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    joined
    .filter(F.col("read_at_percent_rank") < 0.7)
    .select(
        "id",
        "voltage",
        "energy_cost",
        "read_at_percent_rank",
        "site_name",
        "site_commissioned_on",
    )
    .orderBy(F.col("voltage").asc_nulls_last())
)
