from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_voltage_running = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_voltage = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id")

recent = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

derived = (
    recent
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        "voltage_filled",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        F.initcap(F.trim(F.col("quality_prefix"))).alias("quality_prefix_clean"),
    )
)

ranked = (
    derived
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        "voltage_per_kwh",
        "quality_prefix_clean",
        F.first("voltage_per_kwh", ignorenulls=True).over(w_quality_by_voltage_running).alias("first_known_voltage_per_kwh"),
        F.dense_rank().over(w_quality_by_voltage).alias("voltage_dense_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        "voltage_per_kwh",
        "quality_prefix_clean",
        "voltage_dense_rank",
    )
    .orderBy("voltage_per_kwh")
)
