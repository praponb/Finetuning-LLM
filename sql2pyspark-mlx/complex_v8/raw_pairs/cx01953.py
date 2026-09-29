from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy("read_at_month", "id")

prepared_readings = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp")) | (F.col("kwh").between(26.9, 109.0)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
        F.when(F.col("voltage") >= 229.9, "HIGH").when(F.col("voltage") >= 219.5, "MEDIUM").otherwise("LOW").alias("voltage_tier"),
    )
)

tagged = (
    prepared_readings
    .select(
        "*",
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.date_format(F.col("read_at_month"), "yyyy-MM-dd").alias("read_at_month_label"),
    )
)

joined = (
    tagged.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.read_at_month",
        "s.voltage_tier",
        "s.read_at_month_label",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

latest_per_key = (
    joined
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select(
        "id",
        F.col("site_installed_kw").alias("site_installed_kw_v"),
        F.col("kwh").alias("kwh_v"),
    )
    .unpivot(
        ids=["id"],
        values=["site_installed_kw_v", "kwh_v"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
