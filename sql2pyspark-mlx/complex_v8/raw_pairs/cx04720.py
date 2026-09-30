from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_zone", "site_id").orderBy(F.col("kwh").desc(), F.col("id").desc())

recent = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("quality").isin("EST", "BAD")))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
    )
)

with_parent = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.quality_slug",
        F.col("si.zone").alias("site_zone"),
    )
)

qualified = (
    with_parent.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") <= 92.5), "left_semi")
)

latest_per_key = (
    qualified
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .groupingSets(
        [["site_id", "site_zone", "quality"], ["site_id"], ["site_zone"], []],
        "site_id", "site_zone", "quality",
    )
    .agg(
        F.grouping("site_id").alias("is_total_site_id"),
        F.round(100 * F.count(F.when(F.col("site_zone") != "Z3", 1)) / F.count("*"), 1).alias("pct_site_zone_z3"),
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("site_zone").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
