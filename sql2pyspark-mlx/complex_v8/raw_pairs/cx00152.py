from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage_ties = Window.orderBy("voltage")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_read_at_month_site_id = Window.partitionBy(F.month("read_at"), "site_id")
w_all_by_voltage_full = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_dedup = Window.partitionBy("site_tariff").orderBy(F.col("voltage_seq").desc(), F.col("id").desc())

base_readings = (
    df_meter_readings
    .filter((F.col("kwh") >= 51.0) | (F.col("site_id") >= 6))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

sequenced = (
    base_readings
    .select(
        "*",
        F.rank().over(w_all_by_voltage_ties).alias("voltage_rank"),
        F.row_number().over(w_all_by_voltage).alias("voltage_seq"),
        F.size(F.collect_set("quality").over(w_read_at_month_site_id)).alias("distinct_quality_in_read_at_month_site_id"),
        F.last("read_at").over(w_all_by_voltage_full).alias("last_read_at"),
        F.ntile(10).over(w_all_by_voltage).alias("voltage_decile"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_read_at_month_site_id), F.lit(0)), 4).alias("voltage_share_of_read_at_month_site_id"),
    )
)

enriched = (
    sequenced.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.tariff").alias("site_tariff"))
)

filtered = (
    enriched.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.month(F.col("m.read_at")).isin(2, 3)) & (F.col("m.quality").isin("OK", "EST")), "left_anti")
    .filter(F.col("quality") == "BAD")
)

scored = (
    filtered
    .withColumn("kwh_per_voltage_minus_voltage", F.col("kwh_per_voltage") - F.col("voltage"))
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_children = (
    latest.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.kwh_per_voltage",
        "s.distinct_quality_in_read_at_month_site_id",
        "s.voltage_decile",
        "s.voltage_share_of_read_at_month_site_id",
        "s.kwh_per_voltage_minus_voltage",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
    )
)

result = (
    with_children
    .select(
        "id",
        "reading_id",
        F.col("kwh_per_voltage_minus_voltage").alias("kwh_per_voltage_minus_voltage_v"),
        F.col("voltage_share_of_read_at_month_site_id").alias("voltage_share_of_read_at_month_site_id_v"),
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["kwh_per_voltage_minus_voltage_v", "voltage_share_of_read_at_month_site_id_v"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .orderBy("id", "reading_id", "field")
)
