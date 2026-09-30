from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_read_at_month_readin = Window.partitionBy(F.date_trunc("month", "reading_read_at"), "reading_id").orderBy("reading_read_at")
w_reading_quality_read_at_mont = Window.partitionBy("reading_quality", F.year("read_at_month"))

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

stats = (
    joined_ranges
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"), "site_id")
    .agg(
        F.array_sort(F.collect_set("zone")).alias("zone_list"),
        F.round(F.sum("voltage"), 2).alias("total_voltage"),
        F.countDistinct("zone").alias("distinct_zones"),
    )
)

line_level = (
    stats.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.read_at_month",
        "s.site_id",
        "s.zone_list",
        "s.total_voltage",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

windowed = (
    line_level
    .select(
        "*",
        F.rank().over(w_reading_read_at_month_readin).alias("reading_read_at_rank"),
        F.size(F.collect_set("reading_id").over(w_reading_quality_read_at_mont)).alias("distinct_reading_id_in_reading_quality_read_at_m"),
        F.min("total_voltage").over(w_reading_quality_read_at_mont).alias("reading_quality_read_at_month_year_min_total_vol"),
    )
)

result = (
    windowed
    .select(
        "read_at_month",
        "site_id",
        "reading_quality_read_at_month_year_min_total_vol",
        "total_voltage",
        "reading_voltage",
    )
    .unpivot(
        ids=["read_at_month", "site_id"],
        values=["reading_quality_read_at_month_year_min_total_vol", "total_voltage", "reading_voltage"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
)
