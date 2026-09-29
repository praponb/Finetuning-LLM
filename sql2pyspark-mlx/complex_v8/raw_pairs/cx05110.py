from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .filter((F.col("voltage") < 238.3) & (F.col("quality") == "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
)

with_parent = (
    valid.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.quality_group",
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    with_parent
    .groupingSets(
        [["site_id", "quality_group"], ["site_id"]],
        "site_id", "quality_group",
    )
    .agg(
        F.grouping("site_id").alias("is_total_site_id"),
        F.min("kwh").alias("min_kwh"),
    )
    .select(
        "site_id",
        F.coalesce(F.col("quality_group"), F.lit("ALL")).alias("quality_group"),
        "is_total_site_id",
        "min_kwh",
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("quality_group").asc_nulls_last(),
    )
)
