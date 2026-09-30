from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_kwh = Window.partitionBy("quality").orderBy("kwh", "id")
w_quality_by_kwh_ties = Window.partitionBy("quality").orderBy("kwh")
w_voltage_tier = Window.partitionBy("voltage_tier")
w_quality_by_kwh_full = Window.partitionBy("quality").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 239.6, "A").when(F.col("voltage") >= 223.1, "B").otherwise("C").alias("voltage_tier"),
    )
)

metrics = (
    cleaned_readings
    .select(
        "id",
        "site_id",
        "voltage_tier",
        F.lag("voltage").over(w_quality_by_kwh).alias("prev_voltage"),
        F.round(F.cume_dist().over(w_quality_by_kwh_ties), 3).alias("kwh_cume_dist"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_voltage_tier), 2).alias("kwh_vs_voltage_tier_avg"),
        F.nth_value("kwh", 2).over(w_quality_by_kwh_full).alias("second_kwh"),
    )
)

result = (
    metrics
    .select(
        "id",
        "site_id",
        "kwh_cume_dist",
        "second_kwh",
    )
    .orderBy("id", F.col("site_id").desc())
)
