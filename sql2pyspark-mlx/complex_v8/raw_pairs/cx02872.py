from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_zone_by_kwh_pr = Window.partitionBy(F.date_trunc("month", "read_at"), "zone").orderBy("kwh", "id").rowsBetween(-6, -1)
w_read_at_month_zone_by_kwh_ti = Window.partitionBy(F.date_trunc("month", "read_at"), "zone").orderBy("kwh")
w_read_at_month_zone_by_kwh_fu = Window.partitionBy(F.date_trunc("month", "read_at"), "zone").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month_zone_by_kwh = Window.partitionBy(F.date_trunc("month", "read_at"), "zone").orderBy("kwh", "id")

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

metrics = (
    in_window
    .select(
        "*",
        F.max("energy_cost").over(w_read_at_month_zone_by_kwh_pr).alias("energy_cost_max_6_prior"),
        F.round(F.percent_rank().over(w_read_at_month_zone_by_kwh_ti), 3).alias("kwh_percent_rank"),
        F.first("energy_cost").over(w_read_at_month_zone_by_kwh_fu).alias("first_energy_cost"),
        F.round(F.percent_rank().over(w_read_at_month_zone_by_kwh), 3).alias("kwh_percent_rank_2"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
    )
)

summarized_rows = (
    metrics.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at", "m.avg_reading_kwh")
)

qualified = (
    summarized_rows.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (~F.col("m.quality").isin("BAD", "OK")) & (F.col("m.kwh") < 90.6), "left_semi")
)

result = (
    qualified
    .groupBy("site_id")
    .pivot("zone", ["Z2", "Z3", "Z4"])
    .agg(F.max("avg_reading_kwh"))
    .orderBy("site_id")
)
