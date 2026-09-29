from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_by_read_at_moving_2 = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(-2, Window.currentRow)
w_site_id_by_read_at_prior_6 = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(-6, -1)
w_first_reading_read_at_month = Window.partitionBy(F.month("first_reading_read_at"), "site_id").orderBy(F.col("read_at_dense_rank").desc(), "id")
w_first_reading_read_at_month_2 = Window.partitionBy(F.month("first_reading_read_at"), "site_id").orderBy(F.col("read_at_dense_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("voltage").alias("min_reading_voltage"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

with_stats = (
    in_window.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.min_reading_voltage", "m.first_reading_read_at")
)

sequenced = (
    with_stats
    .select(
        "*",
        F.round(F.percent_rank().over(w_site_id_by_read_at), 3).alias("read_at_percent_rank"),
        F.dense_rank().over(w_site_id_by_read_at).alias("read_at_dense_rank"),
        F.lead("read_at", 3).over(w_site_id_by_read_at).alias("next3_read_at"),
        F.round(F.avg("kwh").over(w_site_id_by_read_at_moving_2), 2).alias("kwh_avg_3"),
        F.lead("rate", 2, 0).over(w_site_id_by_read_at).alias("next2_rate"),
        F.max("voltage").over(w_site_id_by_read_at_prior_6).alias("voltage_max_6_prior"),
    )
)

with_trends = (
    sequenced
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "zone",
        "energy_cost",
        "min_reading_voltage",
        "first_reading_read_at",
        "read_at_percent_rank",
        "read_at_dense_rank",
        "next3_read_at",
        "kwh_avg_3",
        "voltage_max_6_prior",
        F.row_number().over(w_first_reading_read_at_month).alias("rn"),
        F.first("next2_rate").over(w_first_reading_read_at_month_2).alias("first_next2_rate"),
        F.round(F.cume_dist().over(w_first_reading_read_at_month), 3).alias("read_at_dense_rank_cume_dist"),
    )
)

result = (
    with_trends
    .filter(F.month(F.col("first_reading_read_at")).isin(1, 3))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "zone",
        "energy_cost",
        "min_reading_voltage",
        "first_reading_read_at",
        "read_at_percent_rank",
        "read_at_dense_rank",
        "kwh_avg_3",
        "first_next2_rate",
        "read_at_dense_rank_cume_dist",
    )
    .orderBy(F.col("first_reading_read_at").asc())
)
