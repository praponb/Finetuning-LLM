from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reading_voltage_prior = Window.orderBy(F.col("reading_voltage").desc(), "id", "reading_id").rowsBetween(-6, -1)
w_reading_id_site_id = Window.partitionBy("reading_id", "site_id")
w_all_by_reading_voltage_runni = Window.orderBy(F.col("reading_voltage").desc(), "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_reading_voltage_ties = Window.orderBy(F.col("reading_voltage").desc())

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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

detail_rows_readings = (
    matched.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.rate",
        "s.energy_cost",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

windowed = (
    detail_rows_readings
    .select(
        "*",
        F.round(F.avg("energy_cost").over(w_all_by_reading_voltage_prior), 2).alias("energy_cost_avg_6_prior"),
        F.round(F.col("energy_cost") / F.nullif(F.sum("energy_cost").over(w_reading_id_site_id), F.lit(0)), 4).alias("energy_cost_share_of_reading_id_site_id"),
        F.round(F.avg("kwh").over(w_reading_id_site_id), 2).alias("reading_id_site_id_avg_kwh"),
        F.max("reading_voltage").over(w_all_by_reading_voltage_runni).alias("max_reading_voltage_so_far"),
        F.round(F.percent_rank().over(w_all_by_reading_voltage_ties), 3).alias("reading_voltage_percent_rank"),
    )
)

labeled = (
    windowed
    .withColumn("days_since_reading_read_at", F.datediff(F.lit("2025-06-30").cast("date"), F.col("reading_read_at")))
)

result = (
    labeled
    .filter(F.col("energy_cost_avg_6_prior") >= 405.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "rate",
        "energy_cost",
        "reading_voltage",
        "reading_read_at",
        "energy_cost_avg_6_prior",
        "max_reading_voltage_so_far",
        "days_since_reading_read_at",
        (F.col("read_at") + F.expr("INTERVAL 2 HOURS")).alias("read_at_plus_2h"),
        F.quarter(F.col("reading_read_at")).alias("reading_read_at_quarter"),
    )
    .orderBy(F.col("reading_read_at").asc_nulls_last())
)
