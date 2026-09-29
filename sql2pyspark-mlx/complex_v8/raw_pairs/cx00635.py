from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_quality_bad_by_n_quality_b = Window.partitionBy("n_quality_bad").orderBy("n_quality_bad", "read_at_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_readings = (
    df_meter_readings
    .filter((F.col("quality") != "EST") & (F.col("site_id") >= 5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
    )
)

labeled = (
    recent_readings
    .withColumn("least_least_kwh_voltage_kwh", F.least(F.col("least_kwh_voltage"), F.col("kwh")))
)

per_group = (
    labeled
    .groupBy(F.date_format("read_at", "yyyy-MM").alias("read_at_ym"))
    .agg(
        F.sum(F.when(F.col("quality") == "BAD", 1).otherwise(0)).alias("n_quality_bad"),
        F.max("voltage").alias("max_voltage"),
        F.sum("least_kwh_voltage").alias("sum_least_kwh_voltage_check"),
    )
    .filter(F.col("sum_least_kwh_voltage_check") > 85.8)
    .drop("sum_least_kwh_voltage_check")
)

with_trends = (
    per_group
    .select(
        "*",
        F.first("max_voltage", ignorenulls=True).over(w_n_quality_bad_by_n_quality_b).alias("first_known_max_voltage"),
    )
)

result = (
    with_trends
    .filter(F.col("read_at_ym").isNotNull())
    .select("read_at_ym", "n_quality_bad", "first_known_max_voltage")
    .orderBy("first_known_max_voltage")
)
