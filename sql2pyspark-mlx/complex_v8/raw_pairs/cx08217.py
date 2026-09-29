from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-19 09:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        (F.col("kwh") + F.col("voltage")).alias("kwh_plus_voltage"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        F.greatest(F.col("voltage"), F.col("kwh")).alias("greatest_voltage_kwh"),
    )
)

labeled = (
    filtered
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
    )
)

combined_details = (
    labeled.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.kwh_plus_voltage",
        "s.kwh_bucket",
        "s.greatest_voltage_kwh",
        "s.quality_slug",
        "s.quality_prefix",
        "s.read_at_label",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
    )
)

derived = (
    combined_details
    .select(
        "id",
        "site_id",
        "voltage",
        "kwh_bucket",
        "greatest_voltage_kwh",
        "quality_slug",
        "quality_prefix",
        "site_installed_kw",
        "site_commissioned_on",
        (F.col("kwh_bucket") + F.col("greatest_voltage_kwh")).alias("kwh_bucket_plus_greatest_voltage_kwh"),
        (F.col("kwh_bucket") - F.col("greatest_voltage_kwh")).alias("kwh_bucket_minus_greatest_voltage_kwh"),
    )
)

result = (
    derived
    .select(
        "id",
        "kwh_bucket_minus_greatest_voltage_kwh",
        "voltage",
        "site_installed_kw",
    )
    .unpivot(
        ids=["id"],
        values=["kwh_bucket_minus_greatest_voltage_kwh", "voltage", "site_installed_kw"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
)
