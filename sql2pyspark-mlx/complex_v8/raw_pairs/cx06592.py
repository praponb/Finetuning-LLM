from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.abs(F.col("kwh") - 57.3).alias("kwh_distance"),
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

tagged = (
    recent
    .select(
        "*",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

rollup_base = (
    tagged
    .filter(F.col("site_id").isNotNull())
    .groupBy("read_at_year")
    .agg(
        F.max("read_at").alias("last_read_at"),
        F.sum("least_voltage_kwh").alias("total_least_voltage_kwh"),
        F.count("*").alias("row_count"),
        F.max("kwh").alias("max_kwh"),
        F.sum("kwh_distance").alias("total_kwh_distance"),
    )
    .filter(F.col("row_count") >= 3)
)

result = (
    rollup_base
    .select("read_at_year", "total_least_voltage_kwh", "total_kwh_distance")
    .unpivot(
        ids=["read_at_year"],
        values=["total_least_voltage_kwh", "total_kwh_distance"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
