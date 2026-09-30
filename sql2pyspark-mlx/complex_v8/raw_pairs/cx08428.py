from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), F.col("id").desc())

valid_readings = (
    df_meter_readings
    .filter(F.col("voltage") < 232.7)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

unique_rows = (
    valid_readings
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.sum("voltage").alias("total_reading_voltage"))
)

with_stats = (
    unique_rows.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.total_reading_voltage")
)

site_id_stats = (
    with_stats
    .groupBy("site_id")
    .agg(F.percentile_approx("quality_len", 0.5).alias("median_quality_len"))
)

compared = (
    with_stats.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.quality_len") >= F.col("g.median_quality_len"))
    .select(
        "s.*",
        "g.median_quality_len",
        F.round(F.col("s.quality_len") - F.col("g.median_quality_len"), 2).alias("quality_len_minus_median"),
    )
)

result = (
    compared
    .select(
        "id",
        F.col("voltage").alias("m_voltage"),
        F.col("voltage_per_kwh").alias("voltage_per_kwh_v"),
        F.col("kwh").alias("kwh_v"),
        F.col("quality_len_minus_median").alias("quality_len_minus_median_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_voltage", "voltage_per_kwh_v", "kwh_v", "quality_len_minus_median_v"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "measure")
)
