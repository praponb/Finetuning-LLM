from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_count_by_reading_cou = Window.partitionBy("reading_count").orderBy("reading_count", "id")
w_reading_count_by_reading_cou_2 = Window.partitionBy("reading_count").orderBy("reading_count")
w_last_reading_read_at_year = Window.partitionBy(F.year("last_reading_read_at"))
w_tariff_reading_count_dense_r = Window.partitionBy("tariff", "reading_count_dense_rank_2")
w_reading_count_dense_rank_2_z = Window.partitionBy("reading_count_dense_rank_2", "zone").orderBy(F.col("reading_count").desc(), "id")
w_all_by_reading_count_ties = Window.orderBy(F.col("reading_count").asc_nulls_last())

recent_sites = (
    df_sites
    .filter(F.length(F.trim(F.col("site_name"))) >= 4)
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        F.upper(F.trim(F.col("site_name"))).alias("site_name_clean"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.concat(F.col("site_name"), F.lit("#"), F.col("id").cast("string")).alias("site_name_ref"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.count("*").alias("reading_count"),
    )
)

summarized_rows = (
    recent_sites.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

filtered = (
    summarized_rows.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.quality") == "OK") & (F.col("m.voltage") > 222.6), "left_anti")
)

qualified = (
    filtered
    .crossJoin(filtered.agg(F.avg("reading_count").alias("overall_avg_reading_count")))
    .filter(F.col("reading_count") > F.col("overall_avg_reading_count"))
    .drop("overall_avg_reading_count")
)

with_windows = (
    qualified
    .select(
        "*",
        F.dense_rank().over(w_reading_count_by_reading_cou).alias("reading_count_dense_rank"),
        F.rank().over(w_reading_count_by_reading_cou_2).alias("reading_count_rank"),
        F.round(F.col("reading_count") - F.avg("reading_count").over(w_last_reading_read_at_year), 2).alias("reading_count_vs_last_reading_read_at_year_avg"),
        F.dense_rank().over(w_reading_count_by_reading_cou_2).alias("reading_count_dense_rank_2"),
    )
)

ranked = (
    with_windows
    .select(
        "*",
        F.size(F.collect_set("tariff_or_unknown").over(w_tariff_reading_count_dense_r)).alias("distinct_tariff_or_unknown_in_tariff_reading_cou"),
        F.ntile(4).over(w_reading_count_dense_rank_2_z).alias("reading_count_quartile"),
        F.round(F.percent_rank().over(w_all_by_reading_count_ties), 3).alias("reading_count_percent_rank"),
        F.lag("last_reading_read_at", 2).over(w_reading_count_dense_rank_2_z).alias("prev2_last_reading_read_at"),
    )
)

result = (
    ranked
    .select(
        "id",
        "tariff",
        "site_name_clean",
        "tariff_or_unknown",
        "site_name_ref",
        "reading_count",
        "reading_count_vs_last_reading_read_at_year_avg",
        "reading_count_dense_rank_2",
        "distinct_tariff_or_unknown_in_tariff_reading_cou",
        "reading_count_quartile",
        "reading_count_percent_rank",
        F.concat_ws(": ", F.col("site_name"), F.col("tariff")).alias("site_name_tariff_label"),
        F.quarter(F.col("last_reading_read_at")).alias("last_reading_read_at_quarter"),
    )
    .orderBy(
        F.col("reading_count_dense_rank_2").asc_nulls_last(),
        F.col("last_reading_read_at_quarter").desc(),
    )
)
