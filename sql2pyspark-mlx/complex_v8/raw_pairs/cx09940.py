from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy("site_commissioned_on", "id")

filtered = (
    df_meter_readings
    .filter(F.col("voltage") <= 232.7)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
)

annotated = (
    filtered
    .filter(F.col("kwh") < 81.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "voltage_per_kwh",
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
    )
)

with_details = (
    annotated.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff").isNotNull())
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.read_at_label",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

joined_site = (
    with_details.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.read_at_label",
        "s.site_commissioned_on",
        F.col("si.zone").alias("site_zone"),
    )
)

annotated_2 = (
    joined_site
    .select(
        "*",
        F.concat_ws(" / ", F.col("read_at_label"), F.col("site_zone")).alias("read_at_label_site_zone_label"),
    )
)

latest = (
    annotated_2
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select(
        "id",
        "site_id",
        "read_at",
        "site_zone",
        "read_at_label_site_zone_label",
        F.when(F.col("site_zone").isin("Z3", "Z4"), "group_a").when(F.col("site_zone") == "Z2", "group_b").otherwise("other").alias("site_zone_group"),
        F.concat_ws("|", F.col("read_at_label"), F.col("site_zone")).alias("read_at_label_site_zone_label_2"),
    )
    .orderBy(F.col("site_id").asc(), "id")
    .limit(20)
)
