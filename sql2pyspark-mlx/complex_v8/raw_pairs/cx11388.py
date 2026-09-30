from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc(), F.col("id").desc())
w_reading_id_by_greatest_volta = Window.partitionBy("reading_id").orderBy("greatest_voltage_kwh", "id", "reading_id")
w_all_by_reading_kwh_full = Window.orderBy(F.col("reading_kwh").desc(), "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_reading_kwh_running = Window.orderBy(F.col("reading_kwh").desc(), "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_year = Window.partitionBy(F.year("read_at"))

recent = (
    df_meter_readings
    .filter((F.col("site_id") >= 2) & (F.month(F.col("read_at")).isin(1, 3)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.greatest(F.col("voltage"), F.col("kwh")).alias("greatest_voltage_kwh"),
    )
)

deduped = (
    recent
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_children_readings = (
    deduped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (~F.col("m.quality").isin("OK", "BAD")), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.greatest_voltage_kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
    )
)

with_windows = (
    with_children_readings
    .select(
        "id",
        "voltage",
        "quality",
        "greatest_voltage_kwh",
        "reading_id",
        "reading_kwh",
        "reading_voltage",
        "reading_quality",
        F.row_number().over(w_reading_id_by_greatest_volta).alias("rn"),
        F.nth_value("voltage", 2).over(w_all_by_reading_kwh_full).alias("second_voltage"),
        F.last("reading_voltage", ignorenulls=True).over(w_all_by_reading_kwh_running).alias("last_known_reading_voltage"),
        F.count(F.lit(1)).over(w_read_at_year).alias("rows_in_read_at_year"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_read_at_year), F.lit(0)), 4).alias("voltage_share_of_read_at_year"),
    )
)

labeled = (
    with_windows
    .select(
        "id",
        "voltage",
        "quality",
        "greatest_voltage_kwh",
        "reading_id",
        "reading_voltage",
        "reading_quality",
        "rn",
        "second_voltage",
        "last_known_reading_voltage",
        "rows_in_read_at_year",
        F.round(F.col("voltage") * 0.9, 2).alias("voltage_adj"),
    )
)

result = (
    labeled
    .select(
        "id",
        "reading_id",
        "rows_in_read_at_year",
        "rn",
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["rows_in_read_at_year", "rn"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
