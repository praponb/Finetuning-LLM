from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("zone").orderBy(F.col("commissioned_on").desc(), F.col("id").desc())
w_tariff_zone_by_commissioned = Window.partitionBy("tariff", "zone").orderBy(F.col("commissioned_on").desc(), "id")
w_tariff_zone = Window.partitionBy("tariff", "zone")
w_all_by_installed_kw = Window.orderBy("installed_kw", "id")
w_tariff_zone_by_commissioned_2 = Window.partitionBy("tariff", "zone").orderBy(F.col("commissioned_on").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_installed_kw_running = Window.orderBy("installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = df_sites.filter(F.col("commissioned_on").isNotNull())

latest = (
    base
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows = (
    latest
    .select(
        "*",
        F.row_number().over(w_tariff_zone_by_commissioned).alias("seq_no"),
        F.row_number().over(w_all_by_installed_kw).alias("seq_no_2"),
        F.round(F.avg("installed_kw").over(w_tariff_zone_by_commissioned_2), 2).alias("installed_kw_avg_2"),
        F.count(F.lit(1)).over(w_tariff_zone).alias("rows_in_tariff_zone"),
        F.min("installed_kw").over(w_all_by_installed_kw_running).alias("min_installed_kw_so_far"),
    )
)

zone_stats = (
    with_windows
    .groupBy("zone")
    .agg(F.avg("seq_no").alias("avg_seq_no"))
)

outliers = (
    with_windows.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.seq_no") > F.col("g.avg_seq_no"))
    .select(
        "s.*",
        "g.avg_seq_no",
        F.round(F.col("s.seq_no") - F.col("g.avg_seq_no"), 2).alias("seq_no_minus_avg"),
    )
)

result = (
    outliers
    .cube("seq_no_2", "seq_no", "tariff")
    .agg(
        F.grouping("seq_no_2").alias("is_total_seq_no_2"),
        F.count("*").alias("row_count"),
        F.countDistinct("tariff").alias("distinct_tariffs"),
        F.min("seq_no").alias("min_seq_no"),
    )
    .orderBy(
        F.col("seq_no_2").asc_nulls_last(),
        F.col("seq_no").asc_nulls_last(),
        F.col("tariff").asc_nulls_last(),
    )
)
