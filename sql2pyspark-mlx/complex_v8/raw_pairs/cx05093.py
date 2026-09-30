from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_quality_site_id_by_r = Window.partitionBy("reading_quality", "site_id").orderBy("read_at", "id", "reading_id")
w_read_at_date_year_by_reading = Window.partitionBy(F.year("read_at_date")).orderBy(F.col("reading_read_at").desc(), "id", "reading_id").rowsBetween(-2, Window.currentRow)
w_read_at_date_year_by_reading_2 = Window.partitionBy(F.year("read_at_date")).orderBy(F.col("reading_read_at").desc(), "id", "reading_id")
w_quality_by_read_at_dense_ran = Window.partitionBy("quality").orderBy("read_at_dense_rank", "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_read_at_dense_ran_2 = Window.partitionBy("quality").orderBy("read_at_dense_rank")
w_quality_by_read_at_dense_ran_3 = Window.partitionBy("quality").orderBy("read_at_dense_rank", "id", "reading_id")
w_site_tariff = Window.partitionBy("site_tariff")

filtered = (
    df_meter_readings
    .filter((F.col("quality").isNotNull()) & (F.col("read_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

with_children_readings = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.month(F.col("m.read_at")).isin(2, 3)), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.read_at_date",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

without_matches = (
    with_children_readings
    .crossJoin(with_children_readings.agg(F.min("voltage").alias("overall_min_voltage")))
    .filter(F.col("voltage") > F.col("overall_min_voltage"))
    .drop("overall_min_voltage")
)

sequenced = (
    without_matches
    .select(
        "*",
        F.lead("voltage").over(w_reading_quality_site_id_by_r).alias("next_voltage"),
        F.round(F.avg("reading_voltage").over(w_read_at_date_year_by_reading), 2).alias("reading_voltage_avg_3"),
        F.dense_rank().over(w_read_at_date_year_by_reading_2).alias("reading_read_at_dense_rank"),
        (F.col("voltage") - F.lag("voltage").over(w_reading_quality_site_id_by_r)).alias("voltage_change"),
        F.dense_rank().over(w_reading_quality_site_id_by_r).alias("read_at_dense_rank"),
    )
)

combined_details = (
    sequenced.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone").isin("Z4", "Z3"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.read_at_date",
        "s.reading_id",
        "s.reading_quality",
        "s.next_voltage",
        "s.reading_voltage_avg_3",
        "s.voltage_change",
        "s.read_at_dense_rank",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

sequenced_2 = (
    combined_details
    .select(
        "id",
        "read_at",
        "quality",
        "read_at_date",
        "reading_id",
        "reading_quality",
        "next_voltage",
        "read_at_dense_rank",
        "site_tariff",
        F.first("voltage_change", ignorenulls=True).over(w_quality_by_read_at_dense_ran).alias("first_known_voltage_change"),
        F.dense_rank().over(w_quality_by_read_at_dense_ran_2).alias("read_at_dense_rank_dense_rank"),
        F.dense_rank().over(w_quality_by_read_at_dense_ran_3).alias("read_at_dense_rank_dense_rank_2"),
        F.count(F.lit(1)).over(w_site_tariff).alias("rows_in_site_tariff"),
        F.rank().over(w_quality_by_read_at_dense_ran_3).alias("read_at_dense_rank_rank"),
        F.size(F.collect_set("site_zone").over(w_site_tariff)).alias("distinct_site_zone_in_site_tariff"),
    )
)

joined = (
    sequenced_2.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "inner")
    .filter(F.col("m.kwh") <= 109.0)
    .select(
        "s.id",
        "s.read_at",
        "s.quality",
        "s.read_at_date",
        "s.reading_id",
        "s.next_voltage",
        "s.site_tariff",
        "s.first_known_voltage_change",
        "s.read_at_dense_rank_dense_rank",
        "s.read_at_dense_rank_dense_rank_2",
        "s.rows_in_site_tariff",
        "s.read_at_dense_rank_rank",
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

result = (
    joined
    .select(
        "id",
        "reading_id",
        "rows_in_site_tariff",
        "read_at_dense_rank_dense_rank",
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["rows_in_site_tariff", "read_at_dense_rank_dense_rank"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
