from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_quality_count_by_rea = Window.partitionBy("reading_quality_count").orderBy(F.col("reading_quality_count").asc_nulls_last(), "id")
w_site_id = Window.partitionBy("site_id")
w_reading_quality_count_quinti = Window.partitionBy("reading_quality_count_quintile").orderBy(F.col("least_voltage_kwh").asc_nulls_last(), "id")
w_reading_quality_count_quinti_2 = Window.partitionBy("reading_quality_count_quintile").orderBy(F.col("least_voltage_kwh").asc_nulls_last())
w_reading_quality_count_quinti_3 = Window.partitionBy("reading_quality_count_quintile")
w_reading_quality_count_quinti_4 = Window.partitionBy("reading_quality_count_quintile").orderBy(F.col("least_voltage_kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_meter_readings
    .filter(F.col("kwh") < 111.9)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(~F.col("quality").isin("BAD", "OK"), 1)).alias("reading_quality_count"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

sequenced = (
    with_stats
    .select(
        "id",
        "kwh",
        "least_voltage_kwh",
        "quality_ref",
        "kwh_filled",
        F.ntile(5).over(w_reading_quality_count_by_rea).alias("reading_quality_count_quintile"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.round(F.percent_rank().over(w_reading_quality_count_quinti), 3).alias("least_voltage_kwh_percent_rank"),
        F.dense_rank().over(w_reading_quality_count_quinti_2).alias("least_voltage_kwh_dense_rank"),
        F.round(F.percent_rank().over(w_reading_quality_count_quinti_2), 3).alias("least_voltage_kwh_percent_rank_2"),
        F.count(F.lit(1)).over(w_reading_quality_count_quinti_3).alias("rows_in_reading_quality_count_quintile"),
        F.sum("least_voltage_kwh").over(w_reading_quality_count_quinti_4).alias("running_least_voltage_kwh"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "kwh",
        "least_voltage_kwh",
        "quality_ref",
        "kwh_filled",
        "reading_quality_count_quintile",
        "voltage_share_of_site_id",
        "least_voltage_kwh_percent_rank",
        "least_voltage_kwh_percent_rank_2",
        "running_least_voltage_kwh",
    )
)
