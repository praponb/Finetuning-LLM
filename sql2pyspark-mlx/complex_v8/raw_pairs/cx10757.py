from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_site_tariff = Window.partitionBy("site_id", "site_tariff")
w_site_tariff_site_id = Window.partitionBy("site_tariff", "site_id")
w_site_tariff_by_site_commissi = Window.partitionBy("site_tariff").orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_tariff_by_site_commissi_2 = Window.partitionBy("site_tariff").orderBy(F.col("site_commissioned_on").desc(), "id")
w_site_tariff_by_site_commissi_3 = Window.partitionBy("site_tariff").orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(-1, -1)
w_all_by_voltage_vs_site_tarif = Window.orderBy("voltage_vs_site_tariff_site_id_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_tariff_site_id_by_site = Window.partitionBy("site_tariff", "site_id").orderBy("site_tariff_site_id_max_kwh")
w_site_tariff_site_id_by_site_2 = Window.partitionBy("site_tariff", "site_id").orderBy("site_tariff_site_id_max_kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage").between(220.7, 238.3)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

with_parent = (
    cleaned_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") != "Z2"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

sequenced = (
    with_parent
    .select(
        "*",
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id_site_tariff), F.lit(0)), 4).alias("voltage_share_of_site_id_site_tariff"),
        F.max("kwh").over(w_site_tariff_site_id).alias("site_tariff_site_id_max_kwh"),
        F.first("kwh", ignorenulls=True).over(w_site_tariff_by_site_commissi).alias("first_known_kwh"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_tariff_site_id), 2).alias("voltage_vs_site_tariff_site_id_avg"),
        F.rank().over(w_site_tariff_by_site_commissi_2).alias("site_commissioned_on_rank"),
        F.round(F.avg("voltage").over(w_site_tariff_by_site_commissi_3), 2).alias("voltage_avg_1_prior"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_tariff_site_id), 2).alias("kwh_vs_site_tariff_site_id_avg"),
    )
)

top_rows = (
    sequenced
    .filter((F.col("site_commissioned_on_rank") <= 5) & (F.col("site_commissioned_on") >= F.lit("2024-09-15").cast("date")))
    .drop("site_commissioned_on_rank")
)

windowed = (
    top_rows
    .select(
        "id",
        "kwh",
        "voltage",
        "site_tariff",
        "site_commissioned_on",
        "voltage_share_of_site_id_site_tariff",
        "first_known_kwh",
        "voltage_vs_site_tariff_site_id_avg",
        "voltage_avg_1_prior",
        "kwh_vs_site_tariff_site_id_avg",
        F.last("site_commissioned_on", ignorenulls=True).over(w_all_by_voltage_vs_site_tarif).alias("last_known_site_commissioned_on"),
        F.round(F.avg("site_tariff_site_id_max_kwh").over(w_site_tariff_site_id), 2).alias("site_tariff_site_id_avg_site_tariff_site_id_max_"),
        F.rank().over(w_site_tariff_site_id_by_site).alias("site_tariff_site_id_max_kwh_rank"),
        F.nth_value("kwh", 2).over(w_site_tariff_site_id_by_site_2).alias("second_kwh"),
    )
)

result = (
    windowed
    .select(
        "id",
        "kwh",
        "voltage",
        "site_tariff",
        "site_commissioned_on",
        "voltage_share_of_site_id_site_tariff",
        "first_known_kwh",
        "voltage_vs_site_tariff_site_id_avg",
        "kwh_vs_site_tariff_site_id_avg",
        "last_known_site_commissioned_on",
        "site_tariff_site_id_avg_site_tariff_site_id_max_",
        "site_tariff_site_id_max_kwh_rank",
        "second_kwh",
    )
    .orderBy(
        F.col("voltage").asc_nulls_last(),
        "voltage_share_of_site_id_site_tariff",
    )
)
