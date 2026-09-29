from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_kwh_running = Window.partitionBy("quality").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality = Window.partitionBy("quality")

cleaned_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage") > 224.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
    )
)

labeled = (
    cleaned_readings
    .select(
        "*",
        (F.floor(F.col("least_voltage_kwh") / 50) * 50).alias("least_voltage_kwh_bucket"),
        F.coalesce(F.col("least_voltage_kwh"), F.lit(0.0)).alias("least_voltage_kwh_filled"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

sequenced = (
    labeled
    .select(
        "*",
        F.sum("least_voltage_kwh_filled").over(w_quality_by_kwh_running).alias("running_least_voltage_kwh_filled"),
        F.max("kwh").over(w_quality).alias("quality_max_kwh"),
        F.last("kwh", ignorenulls=True).over(w_quality_by_kwh_running).alias("last_known_kwh"),
    )
)

qualified = (
    sequenced.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") >= 224.9) & (~F.col("m.quality").isin("EST", "BAD")), "left_semi")
)

result = (
    qualified
    .groupBy("read_at", "quality")
    .pivot("quality_or_unknown", ["EST", "OK"])
    .agg(F.sum("least_voltage_kwh"))
)
