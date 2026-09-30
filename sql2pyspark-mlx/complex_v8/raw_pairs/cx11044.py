from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter((F.col("kwh") > 51.0) | (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

annotated = (
    base_readings
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

derived = (
    annotated
    .filter(F.col("read_at") >= F.lit("2025-02-06 13:00:00").cast("timestamp"))
    .withColumn("days_since_read_at", F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")))
)

derived_2 = (
    derived
    .filter(F.col("days_since_read_at") < 525)
    .select(
        "*",
        F.coalesce(F.col("days_since_read_at"), F.lit(0)).alias("days_since_read_at_filled"),
        F.concat(F.col("quality"), F.lit("#"), F.col("read_at_dayofweek").cast("string")).alias("quality_ref"),
    )
)

result = (
    derived_2
    .select("id", "kwh", "kwh_filled")
    .unpivot(
        ids=["id"],
        values=["kwh", "kwh_filled"],
        variableColumnName="field",
        valueColumnName="value",
    )
)
