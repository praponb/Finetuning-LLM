from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .filter((F.col("voltage") >= 218.4) & (F.col("kwh") <= 79.4))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
    )
)

per_group = (
    valid
    .groupBy(
        "site_id",
        "quality",
        F.when(F.col("voltage") >= 232.4, "A").when(F.col("voltage") >= 223.3, "B").otherwise("C").alias("voltage_tier"),
    )
    .agg(
        F.round(F.sum("kwh_minus_voltage"), 2).alias("total_kwh_minus_voltage"),
        F.min("voltage").alias("min_voltage"),
        F.max("kwh_minus_voltage").alias("max_kwh_minus_voltage"),
        F.round(F.stddev("kwh"), 2).alias("stddev_kwh"),
    )
)

result = (
    per_group
    .groupBy("site_id")
    .pivot("voltage_tier", ["C", "B"])
    .agg(F.sum("stddev_kwh"))
    .orderBy("site_id")
)
