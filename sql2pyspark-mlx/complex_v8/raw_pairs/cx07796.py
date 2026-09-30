from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")) & (F.col("voltage").between(215.6, 237.8)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.round(F.col("voltage") * 1.07, 2).alias("voltage_adj"),
    )
)

with_activity = (
    prepared_readings
    .crossJoin(prepared_readings.agg(F.min("kwh").alias("overall_min_kwh")))
    .filter(F.col("kwh") > F.col("overall_min_kwh"))
    .drop("overall_min_kwh")
)

totals = (
    with_activity
    .groupBy("site_id", F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(
        F.min("read_at").alias("first_read_at"),
        F.max("read_at").alias("last_read_at"),
        F.round(F.sum("kwh") / F.nullif(F.sum("voltage_adj"), F.lit(0)), 3).alias("kwh_to_voltage_adj_ratio"),
        F.max("voltage_adj").alias("max_voltage_adj_check"),
    )
    .filter(F.col("max_voltage_adj_check") > 273.0)
    .drop("max_voltage_adj_check")
)

enriched = (
    totals
    .filter(F.col("site_id") >= 9)
    .select(
        "site_id",
        "read_at_month",
        "first_read_at",
        "last_read_at",
        "kwh_to_voltage_adj_ratio",
        F.to_date(F.col("last_read_at")).alias("last_read_at_date"),
        (F.floor(F.col("kwh_to_voltage_adj_ratio") / 2) * 2).alias("kwh_to_voltage_adj_ratio_bucket"),
    )
)

result = (
    enriched
    .select(
        "site_id",
        "read_at_month",
        "first_read_at",
        "last_read_at",
        "kwh_to_voltage_adj_ratio",
        "kwh_to_voltage_adj_ratio_bucket",
        F.when(F.col("kwh_to_voltage_adj_ratio") >= 5.5, "HIGH").when(F.col("kwh_to_voltage_adj_ratio") >= 3.5, "MEDIUM").otherwise("LOW").alias("kwh_to_voltage_adj_ratio_tier"),
        F.round(F.col("kwh_to_voltage_adj_ratio") * 1.5, 2).alias("kwh_to_voltage_adj_ratio_adj"),
    )
    .orderBy(F.col("first_read_at").asc_nulls_last(), "kwh_to_voltage_adj_ratio_adj")
)
