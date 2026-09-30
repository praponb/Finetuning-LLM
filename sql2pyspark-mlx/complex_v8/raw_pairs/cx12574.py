from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_site_commissioned_on = Window.partitionBy("site_id", F.date_trunc("month", "site_commissioned_on")).orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_tariff_quality_by_kwh_r = Window.partitionBy("site_tariff", "quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_tariff_quality_by_kwh = Window.partitionBy("site_tariff", "quality").orderBy(F.col("kwh").desc(), "id")

scoped = (
    df_meter_readings
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-02-06").cast("date"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
)

enriched_site = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

filtered = (
    enriched_site.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") <= 81.9), "left_anti")
    .filter(F.col("site_id").isNotNull())
)

metrics = (
    filtered
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "site_name",
        "site_commissioned_on",
        F.min("kwh").over(w_site_id_site_commissioned_on).alias("min_kwh_so_far"),
        F.max("kwh").over(w_site_tariff_quality_by_kwh_r).alias("max_kwh_so_far"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_site_tariff_quality_by_kwh))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.sum("kwh").over(w_site_id_site_commissioned_on).alias("running_kwh"),
    )
)

derived = (
    metrics
    .filter(F.col("max_kwh_so_far") < 720.0)
    .select(
        "*",
        F.when(F.col("max_kwh_so_far") >= 960.0, "A").when(F.col("max_kwh_so_far") >= 480.0, "B").otherwise("C").alias("max_kwh_so_far_tier"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("site_commissioned_on")).alias("days_since_site_commissioned_on"),
    )
)

result = (
    derived
    .select("id", "kwh", "voltage")
    .unpivot(
        ids=["id"],
        values=["kwh", "voltage"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
