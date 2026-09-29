from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff_quality = Window.partitionBy("site_tariff", "quality")
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_site_id = Window.partitionBy("site_id")
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy("voltage", "id")
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")
w_quality_distinct_site_id_in = Window.partitionBy("quality", "distinct_site_id_in_site_tariff_quality").orderBy(F.col("kwh_change").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_site_id_site_tariff = Window.partitionBy("site_id", "site_tariff")
w_quality_distinct_site_id_in_2 = Window.partitionBy("quality", "distinct_site_id_in_site_tariff_quality").orderBy(F.col("kwh_change").desc_nulls_first())

filtered = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 0.9, 2).alias("kwh_adj"),
    )
)

enriched_site = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.kwh_adj",
        F.col("si.tariff").alias("site_tariff"),
    )
)

metrics = (
    enriched_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "kwh_adj",
        "site_tariff",
        F.sum("voltage").over(w_site_tariff_quality).alias("site_tariff_quality_sum_voltage"),
        F.lead("kwh_adj", 3, 0).over(w_all_by_kwh).alias("next3_kwh_adj"),
        F.size(F.collect_set("site_id").over(w_site_tariff_quality)).alias("distinct_site_id_in_site_tariff_quality"),
        F.sum("kwh_adj").over(w_site_id).alias("site_id_sum_kwh_adj"),
        F.round(F.avg("kwh").over(w_site_id), 2).alias("site_id_avg_kwh"),
        F.dense_rank().over(w_site_id_by_voltage).alias("voltage_dense_rank"),
        (F.col("kwh") - F.lag("kwh").over(w_all_by_voltage)).alias("kwh_change"),
        F.row_number().over(w_all_by_voltage).alias("voltage_seq"),
        F.lead("kwh").over(w_all_by_kwh).alias("next_kwh"),
    )
)

metrics_2 = (
    metrics
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "kwh_adj",
        "site_tariff",
        "site_tariff_quality_sum_voltage",
        "next3_kwh_adj",
        "distinct_site_id_in_site_tariff_quality",
        "site_id_sum_kwh_adj",
        "site_id_avg_kwh",
        "kwh_change",
        "voltage_seq",
        "next_kwh",
        F.round(F.avg("next3_kwh_adj").over(w_quality_distinct_site_id_in), 2).alias("next3_kwh_adj_avg_2_prior"),
        F.round(F.col("site_id_sum_kwh_adj") - F.avg("site_id_sum_kwh_adj").over(w_site_id_site_tariff), 2).alias("site_id_sum_kwh_adj_vs_site_id_site_tariff_avg"),
        F.size(F.collect_set("quality").over(w_site_id_site_tariff)).alias("distinct_quality_in_site_id_site_tariff"),
        F.dense_rank().over(w_quality_distinct_site_id_in_2).alias("kwh_change_dense_rank"),
    )
)

result = (
    metrics_2
    .filter(F.col("site_id_avg_kwh") < 78.0)
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "site_tariff_quality_sum_voltage",
        "next3_kwh_adj",
        "distinct_site_id_in_site_tariff_quality",
        "site_id_sum_kwh_adj",
        "site_id_avg_kwh",
        "kwh_change",
        "next_kwh",
        "next3_kwh_adj_avg_2_prior",
        "site_id_sum_kwh_adj_vs_site_id_site_tariff_avg",
        "kwh_change_dense_rank",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        (F.floor(F.col("kwh_change") / 50) * 50).alias("kwh_change_bucket"),
    )
    .orderBy(F.col("quality_prefix").asc_nulls_last(), "id")
    .limit(10)
)
