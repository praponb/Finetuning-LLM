from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("reading_quality", "site_id").orderBy(F.col("read_at").desc(), F.col("id").desc(), F.col("reading_id").desc())

base_readings = (
    df_meter_readings
    .filter((F.col("quality").isNotNull()) & (F.col("kwh") < 116.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.when(F.col("voltage") >= 234.7, "HIGH").when(F.col("voltage") >= 222.6, "MEDIUM").otherwise("LOW").alias("voltage_tier"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

detail_rows = (
    base_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "BAD"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

qualified = (
    detail_rows.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") < 83.8) & (F.col("m.quality").isNotNull()), "left_semi")
)

unique_rows = (
    qualified
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .groupingSets(
        [["site_id", "reading_quality", "reading_id"], ["site_id"], []],
        "site_id", "reading_quality", "reading_id",
    )
    .agg(
        F.array_sort(F.collect_set("reading_quality")).alias("reading_quality_list"),
        F.sum(F.when(F.col("reading_quality") == "EST", 1).otherwise(0)).alias("n_reading_quality_est"),
    )
)
