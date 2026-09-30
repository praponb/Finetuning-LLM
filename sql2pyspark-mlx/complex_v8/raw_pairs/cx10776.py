from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_read_at = Window.partitionBy("site_id").orderBy(F.col("read_at").asc_nulls_last(), "id")
w_quality_site_tariff_by_read = Window.partitionBy("quality", "site_tariff").orderBy("read_at", "id")
w_quality = Window.partitionBy("quality")
w_all_by_read_at_ties = Window.orderBy("read_at")
w_quality_site_tariff_by_read_2 = Window.partitionBy("quality", "site_tariff").orderBy("read_at", "id").rowsBetween(-1, -1)
w_all_by_site_commissioned_on = Window.orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_site_tariff = Window.partitionBy("site_id", "site_tariff")

valid = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")) & (F.col("voltage") < 232.7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

enriched = (
    valid.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "site_commissioned_on",
        "site_tariff",
        "site_name",
        F.dense_rank().over(w_site_id_by_read_at).alias("read_at_dense_rank"),
        F.row_number().over(w_quality_site_tariff_by_read).alias("read_at_seq"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality), 2).alias("kwh_vs_quality_avg"),
        F.round(F.percent_rank().over(w_all_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.round(F.avg("kwh").over(w_quality), 2).alias("quality_avg_kwh"),
        F.sum("kwh").over(w_quality_site_tariff_by_read_2).alias("kwh_sum_1_prior"),
        F.lead("site_commissioned_on", 3).over(w_site_id_by_read_at).alias("next3_site_commissioned_on"),
        F.size(F.collect_set("site_tariff").over(w_quality)).alias("distinct_site_tariff_in_quality"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.min("kwh").over(w_quality).alias("quality_min_kwh"),
        F.rank().over(w_all_by_read_at_ties).alias("read_at_rank"),
        F.last("kwh", ignorenulls=True).over(w_all_by_site_commissioned_on).alias("last_known_kwh"),
        F.max("kwh").over(w_site_id_site_tariff).alias("site_id_site_tariff_max_kwh"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "site_commissioned_on",
        "site_tariff",
        "site_name",
        "read_at_seq",
        "kwh_vs_quality_avg",
        "read_at_percent_rank",
        "quality_avg_kwh",
        "kwh_sum_1_prior",
        "next3_site_commissioned_on",
        "distinct_site_tariff_in_quality",
        "distinct_site_id_in_quality",
        "quality_min_kwh",
        "last_known_kwh",
        "site_id_site_tariff_max_kwh",
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
    .orderBy(F.col("id").desc())
)
