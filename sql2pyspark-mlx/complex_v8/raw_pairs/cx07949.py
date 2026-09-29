from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage_running = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")
w_quality_site_id = Window.partitionBy("quality", "site_id")

base = (
    df_meter_readings
    .filter((F.col("quality") != "BAD") & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

combined_details_site = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        F.col("si.zone").alias("site_zone"),
    )
)

metrics = (
    combined_details_site
    .select(
        "*",
        F.sum("voltage").over(w_all_by_voltage_running).alias("running_voltage"),
        F.lead("voltage").over(w_all_by_voltage).alias("next_voltage"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_voltage)).alias("voltage_change"),
        F.ntile(5).over(w_all_by_voltage).alias("voltage_quintile"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_site_id), 2).alias("voltage_vs_quality_site_id_avg"),
    )
)

annotated = (
    metrics
    .select(
        "*",
        (F.col("read_at") + F.expr("INTERVAL 1 DAYS")).alias("read_at_plus_1d"),
        F.greatest(F.col("voltage_quintile"), F.col("next_voltage")).alias("greatest_voltage_quintile_next_voltage"),
    )
)

result = (
    annotated
    .select(
        "id",
        F.col("voltage_vs_quality_site_id_avg").alias("voltage_vs_quality_site_id_avg_v"),
        F.col("voltage").alias("voltage_v"),
        F.col("voltage_change").alias("voltage_change_v"),
        F.col("next_voltage").alias("m_next_voltage"),
    )
    .unpivot(
        ids=["id"],
        values=["voltage_vs_quality_site_id_avg_v", "voltage_v", "voltage_change_v", "m_next_voltage"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
