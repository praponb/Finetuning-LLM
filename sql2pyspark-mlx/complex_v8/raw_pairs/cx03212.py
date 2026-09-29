from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter(F.col("kwh") < 81.9)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

scored = (
    cleaned_readings
    .withColumn("voltage_per_kwh", F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3))
)

result = (
    scored
    .groupingSets(
        [["quality", "site_id"], ["quality"], ["site_id"], []],
        "quality", "site_id",
    )
    .agg(
        F.round(F.sum("voltage_per_kwh") / F.nullif(F.sum("voltage"), F.lit(0)), 3).alias("voltage_per_kwh_to_voltage_ratio"),
        F.count("*").alias("n_rows"),
        F.max("read_at").alias("last_read_at"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "site_id",
        "voltage_per_kwh_to_voltage_ratio",
        "n_rows",
        "last_read_at",
    )
    .orderBy(F.col("quality").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
