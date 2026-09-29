from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_running = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality_by_read_at_p = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").desc(), "id").rowsBetween(-1, -1)
w_site_id_quality_by_kwh = Window.partitionBy("site_id", "quality").orderBy("kwh", "id")

prepared = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

sequenced = (
    prepared
    .select(
        "*",
        F.first("voltage_filled", ignorenulls=True).over(w_all_by_read_at_running).alias("first_known_voltage_filled"),
        F.sum("voltage").over(w_site_id_quality_by_read_at_p).alias("voltage_sum_1_prior"),
        F.dense_rank().over(w_site_id_quality_by_kwh).alias("kwh_dense_rank"),
    )
)

with_details_site = (
    sequenced.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff").isNotNull())
    .select("s.*", F.col("si.tariff").alias("site_tariff"))
)

labeled = (
    with_details_site
    .withColumn("site_tariff_ref", F.concat(F.col("site_tariff"), F.lit("#"), F.col("kwh_dense_rank").cast("string")))
)

result = (
    labeled
    .groupingSets(
        [["site_tariff", "site_id", "quality"], ["site_tariff"], ["site_id"]],
        "site_tariff", "site_id", "quality",
    )
    .agg(
        F.grouping("site_tariff").alias("is_total_site_tariff"),
        F.count("*").alias("cnt"),
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.countDistinct("site_tariff").alias("distinct_site_tariffs"),
    )
    .orderBy(
        F.col("site_tariff").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
