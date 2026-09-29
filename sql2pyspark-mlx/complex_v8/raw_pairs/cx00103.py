from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_read_at_to_date_by_r = Window.partitionBy("site_id", F.to_date("read_at")).orderBy(F.col("reading_quality_count").desc_nulls_first())
w_zone = Window.partitionBy("zone")

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

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(~F.col("quality").isin("EST", "OK"), 1)).alias("reading_quality_count"),
        F.max("kwh").alias("max_reading_kwh"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

with_stats = (
    covered.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.max_reading_kwh",
        "m.last_reading_read_at",
    )
)

sequenced = (
    with_stats
    .select(
        "*",
        F.round(F.percent_rank().over(w_site_id_read_at_to_date_by_r), 3).alias("reading_quality_count_percent_rank"),
        F.round(F.col("energy_cost") / F.nullif(F.sum("energy_cost").over(w_zone), F.lit(0)), 4).alias("energy_cost_share_of_zone"),
    )
)

derived = (
    sequenced
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "zone",
        "energy_cost",
        "last_reading_read_at",
        "reading_quality_count_percent_rank",
        "energy_cost_share_of_zone",
        (F.floor(F.col("energy_cost") / 250) * 250).alias("energy_cost_bucket"),
    )
)

result = (
    derived
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "zone",
        "energy_cost",
        "last_reading_read_at",
        "reading_quality_count_percent_rank",
    )
    .orderBy(
        F.col("last_reading_read_at").asc_nulls_last(),
        F.col("kwh").asc_nulls_last(),
    )
)
