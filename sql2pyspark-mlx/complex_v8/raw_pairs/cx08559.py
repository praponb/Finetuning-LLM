from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_tariff_by_kwh_t = Window.partitionBy("quality", "site_tariff").orderBy("kwh")
w_all_by_kwh = Window.orderBy("kwh", "id")
w_site_tariff_quality = Window.partitionBy("site_tariff", "quality")
w_quality_site_tariff_by_volta = Window.partitionBy("quality", "site_tariff").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_site_tariff = Window.partitionBy("site_id", "site_tariff")
w_kwh_dense_rank = Window.partitionBy("kwh_dense_rank")
w_all_by_kwh_cume_dist = Window.orderBy(F.col("kwh_cume_dist").asc_nulls_last(), "id")
w_all_by_kwh_cume_dist_moving = Window.orderBy(F.col("kwh_cume_dist").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_meter_readings
    .filter((F.col("voltage").between(216.8, 234.5)) & (F.to_date(F.col("read_at")) < F.lit("2025-03-02").cast("date")))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

with_details_site = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.tariff").alias("site_tariff"),
    )
)

sequenced = (
    with_details_site
    .select(
        "*",
        F.round(F.cume_dist().over(w_quality_site_tariff_by_kwh_t), 3).alias("kwh_cume_dist"),
        F.dense_rank().over(w_all_by_kwh).alias("kwh_dense_rank"),
        F.size(F.collect_set("site_id").over(w_site_tariff_quality)).alias("distinct_site_id_in_site_tariff_quality"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "voltage",
        "site_tariff",
        "kwh_cume_dist",
        "kwh_dense_rank",
        "distinct_site_id_in_site_tariff_quality",
        F.sum("kwh").over(w_quality_site_tariff_by_volta).alias("running_kwh"),
        F.round(F.col("kwh_cume_dist") / F.nullif(F.sum("kwh_cume_dist").over(w_site_id_site_tariff), F.lit(0)), 4).alias("kwh_cume_dist_share_of_site_id_site_tariff"),
    )
)

derived = (
    sequenced_2
    .filter(F.col("distinct_site_id_in_site_tariff_quality") > 3)
    .withColumn("site_tariff_len", F.length(F.trim(F.col("site_tariff"))))
)

sequenced_3 = (
    derived
    .select(
        "*",
        F.size(F.collect_set("site_tariff").over(w_kwh_dense_rank)).alias("distinct_site_tariff_in_kwh_dense_rank"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_kwh_cume_dist)).alias("voltage_change"),
        F.sum("running_kwh").over(w_all_by_kwh_cume_dist_moving).alias("running_kwh_sum_7"),
    )
)

scored = (
    sequenced_3
    .select(
        "*",
        F.when(F.col("kwh_cume_dist") >= 0.8, "A").when(F.col("kwh_cume_dist") >= 0.4, "B").otherwise("C").alias("kwh_cume_dist_tier"),
        (F.floor(F.col("running_kwh") / 250) * 250).alias("running_kwh_bucket"),
        F.substring(F.trim(F.col("site_tariff")), 1, 2).alias("site_tariff_prefix"),
    )
)

result = (
    scored
    .select(
        "id",
        "voltage",
        "site_tariff",
        "kwh_cume_dist",
        "distinct_site_id_in_site_tariff_quality",
        "running_kwh",
        "kwh_cume_dist_share_of_site_id_site_tariff",
        "site_tariff_len",
        "distinct_site_tariff_in_kwh_dense_rank",
        "voltage_change",
        "running_kwh_sum_7",
        "site_tariff_prefix",
    )
    .orderBy(
        F.col("kwh_cume_dist_share_of_site_id_site_tariff").desc(),
        F.col("running_kwh").desc(),
    )
)
