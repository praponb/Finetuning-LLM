from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_id_zone_by_reading_r = Window.partitionBy("reading_id", "zone").orderBy("reading_read_at", "id", "reading_id")
w_reading_id_zone_by_reading_r_2 = Window.partitionBy("reading_id", "zone").orderBy("reading_read_at", "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

covered = (
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

with_children = (
    covered.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.zone",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
    )
)

with_windows = (
    with_children
    .select(
        "id",
        "site_id",
        "read_at",
        "reading_id",
        "reading_kwh",
        "reading_quality",
        F.row_number().over(w_reading_id_zone_by_reading_r).alias("seq_no"),
        F.round(F.cume_dist().over(w_reading_id_zone_by_reading_r), 3).alias("reading_read_at_cume_dist"),
        F.lead("reading_voltage", 3).over(w_reading_id_zone_by_reading_r).alias("next3_reading_voltage"),
        F.nth_value("reading_kwh", 2).over(w_reading_id_zone_by_reading_r_2).alias("second_reading_kwh"),
    )
)

combined_details = (
    with_windows.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.reading_id",
        "s.reading_quality",
        "s.reading_read_at_cume_dist",
        "s.next3_reading_voltage",
        F.col("m.site_id").alias("reading_site_id"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

result = (
    combined_details
    .groupingSets(
        [["reading_quality", "reading_site_id", "reading_id"], ["reading_quality"]],
        "reading_quality", "reading_site_id", "reading_id",
    )
    .agg(
        F.grouping("reading_quality").alias("is_total_reading_quality"),
        F.count("*").alias("cnt"),
        F.countDistinct("reading_site_id").alias("distinct_reading_site_ids"),
        F.sum(F.when(F.col("reading_quality") == "EST", 1).otherwise(0)).alias("n_reading_quality_est"),
    )
    .select(
        F.coalesce(F.col("reading_quality"), F.lit("ALL")).alias("reading_quality"),
        "reading_site_id",
        "reading_id",
        "is_total_reading_quality",
        "cnt",
        "distinct_reading_site_ids",
        "n_reading_quality_est",
    )
    .orderBy(
        F.col("reading_quality").asc_nulls_last(),
        F.col("reading_site_id").asc_nulls_last(),
        F.col("reading_id").asc_nulls_last(),
    )
)
