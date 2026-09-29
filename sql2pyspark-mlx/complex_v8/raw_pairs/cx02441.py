from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), F.col("id").desc())

filtered_readings = (
    df_meter_readings
    .filter((F.col("voltage") < 233.1) & (F.hour(F.col("read_at")).between(9, 12)))
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

deduped = (
    filtered_readings
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .groupingSets(
        [["quality", "site_id"], ["quality"], ["site_id"]],
        "quality", "site_id",
    )
    .agg(
        F.grouping("quality").alias("is_total_quality"),
        F.max("kwh").alias("max_kwh"),
        F.min("voltage").alias("min_voltage"),
        F.round(F.sum("voltage") / F.nullif(F.sum("least_voltage_kwh"), F.lit(0)), 3).alias("voltage_to_least_voltage_kwh_ratio"),
    )
    .orderBy(F.col("quality").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
