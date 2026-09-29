from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("quality").orderBy("kwh", "id")

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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

unique_rows = (
    joined_ranges
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

totals = (
    unique_rows
    .groupBy("site_id")
    .agg(F.min("rate").alias("min_rate"), F.count("*").alias("row_count"))
    .filter(F.col("row_count") >= 3)
)

exploded_detail = (
    totals.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.site_id",
        "s.min_rate",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    exploded_detail
    .rollup("reading_id", "reading_quality")
    .agg(
        F.min("reading_read_at").alias("first_reading_read_at"),
        F.sum(F.when(~F.col("reading_quality").isin("EST", "BAD"), F.col("reading_kwh"))).alias("reading_kwh_reading_quality_not_in_est_b"),
        F.count(F.when(F.col("reading_quality") == "BAD", 1)).alias("n_reading_quality_bad"),
    )
    .orderBy(
        F.col("reading_id").asc_nulls_last(),
        F.col("reading_quality").asc_nulls_last(),
    )
)
