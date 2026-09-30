from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_voltage_running = Window.partitionBy("quality").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality = Window.partitionBy("quality")
w_quality_by_read_at_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

ranked = (
    recent
    .select(
        "*",
        F.sum("kwh").over(w_quality_by_voltage_running).alias("running_kwh"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.last("voltage", ignorenulls=True).over(w_quality_by_read_at_running).alias("last_known_voltage"),
    )
)

grouped = (
    ranked
    .groupBy("site_id", "quality")
    .agg(
        F.max("read_at").alias("last_read_at"),
        F.max("running_kwh").alias("max_running_kwh"),
        F.round(F.stddev("last_known_voltage"), 2).alias("stddev_last_known_voltage"),
    )
)

with_parent = (
    grouped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.site_id",
        "s.quality",
        "s.last_read_at",
        "s.max_running_kwh",
        "s.stddev_last_known_voltage",
        "si.site_name",
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    with_parent
    .select(
        "site_id",
        "quality",
        F.col("stddev_last_known_voltage").alias("stddev_last_known_voltage_v"),
        F.col("max_running_kwh").alias("m_max_running_kwh"),
    )
    .unpivot(
        ids=["site_id", "quality"],
        values=["stddev_last_known_voltage_v", "m_max_running_kwh"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
