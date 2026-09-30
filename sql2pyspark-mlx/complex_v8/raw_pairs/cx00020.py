from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_id_by_kwh = Window.partitionBy("reading_id").orderBy("kwh", "id", "reading_id")
w_site_id_quality = Window.partitionBy("site_id", "quality")

recent = (
    df_meter_readings
    .filter((F.col("quality").isin("EST", "OK")) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_children = (
    recent.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

labeled = (
    with_children
    .filter(F.col("read_at") < F.lit("2025-02-18 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

enriched = (
    labeled.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.upper(F.trim(F.col("si.site_name"))).like("D%"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.reading_id",
        "s.reading_kwh",
        "s.quality_or_unknown",
        "s.days_since_read_at",
        "s.kwh_filled",
        "si.site_name",
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.lead("read_at", 3).over(w_reading_id_by_kwh).alias("next3_read_at"),
        F.round(F.col("kwh_filled") - F.avg("kwh_filled").over(w_site_id_quality), 2).alias("kwh_filled_vs_site_id_quality_avg"),
        F.round(F.col("kwh_filled") / F.nullif(F.sum("kwh_filled").over(w_site_id_quality), F.lit(0)), 4).alias("kwh_filled_share_of_site_id_quality"),
        F.size(F.collect_set("reading_id").over(w_site_id_quality)).alias("distinct_reading_id_in_site_id_quality"),
    )
)

result = (
    ranked
    .select(
        "id",
        "reading_id",
        F.col("reading_kwh").alias("reading_kwh_v"),
        F.col("kwh_filled").alias("kwh_filled_v"),
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["reading_kwh_v", "kwh_filled_v"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "reading_id", "field")
)
