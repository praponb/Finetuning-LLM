from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("quality").orderBy(F.col("kwh").desc(), F.col("id").desc())
w_site_id_quality_group_by_rea = Window.partitionBy("site_id", "quality_group").orderBy(F.col("read_at").desc())

filtered_readings = (
    df_meter_readings
    .filter((F.col("quality") == "OK") & (F.col("kwh") <= 109.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

deduped = (
    filtered_readings
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

annotated = (
    deduped
    .select(
        "*",
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

without_matches = (
    annotated.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") != "BAD"), "left_anti")
    .filter(F.lower(F.col("quality_clean")).like("%ad%"))
)

sequenced = (
    without_matches
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "quality_group",
        "greatest_kwh_voltage",
        F.round(F.percent_rank().over(w_site_id_quality_group_by_rea), 3).alias("read_at_percent_rank"),
        F.round(F.cume_dist().over(w_site_id_quality_group_by_rea), 3).alias("read_at_cume_dist"),
    )
)

with_details_site = (
    sequenced.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality_group",
        "s.greatest_kwh_voltage",
        "s.read_at_percent_rank",
        "s.read_at_cume_dist",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    with_details_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_group",
        "greatest_kwh_voltage",
        "read_at_percent_rank",
        "read_at_cume_dist",
        "site_zone",
        "site_commissioned_on",
        "site_tariff",
    )
    .orderBy(F.col("read_at").desc())
)
