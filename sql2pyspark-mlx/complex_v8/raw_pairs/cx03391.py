from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_commissioned_on_month_b = Window.partitionBy(F.month("site_commissioned_on")).orderBy(F.col("kwh").desc(), "id")
w_site_commissioned_on_month_b_2 = Window.partitionBy(F.month("site_commissioned_on")).orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id = Window.partitionBy("site_id")
w_site_commissioned_on_month_s = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"), "site_id").orderBy(F.col("kwh").desc(), "id")
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_site_commissioned = Window.partitionBy("site_id").orderBy(F.col("site_commissioned_on").desc_nulls_first())
w_site_id_by_site_commissioned_2 = Window.partitionBy("site_id").orderBy(F.col("site_commissioned_on").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_site_id_by_site_commissioned_3 = Window.partitionBy("site_id").orderBy(F.col("site_commissioned_on").desc_nulls_first(), "id")
w_site_commissioned_on_month_s_2 = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"), "site_id").orderBy(F.col("kwh").desc())
w_all_by_kwh_ties = Window.orderBy(F.col("kwh").desc())
w_site_commissioned_on_month = Window.partitionBy(F.month("site_commissioned_on"))
w_kwh_tercile_kwh_quartile = Window.partitionBy("kwh_tercile", "kwh_quartile")
w_kwh_tercile_by_kwh_ties = Window.partitionBy("kwh_tercile").orderBy("kwh")

base_readings = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(2, 3)) & (F.col("voltage") < 233.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

enriched = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on") >= F.lit("2024-08-24").cast("date"))
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality_clean",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

ranked = (
    enriched
    .select(
        "id",
        "kwh",
        "site_commissioned_on",
        F.ntile(4).over(w_site_commissioned_on_month_b).alias("kwh_quartile"),
        F.first("site_commissioned_on", ignorenulls=True).over(w_site_commissioned_on_month_b_2).alias("first_known_site_commissioned_on"),
        F.sum("kwh").over(w_site_id).alias("site_id_sum_kwh"),
        F.ntile(3).over(w_site_commissioned_on_month_s).alias("kwh_tercile"),
        F.dense_rank().over(w_site_commissioned_on_month_b).alias("kwh_dense_rank"),
        F.sum("kwh").over(w_all_by_kwh_running).alias("running_kwh"),
        F.round(F.cume_dist().over(w_site_id_by_site_commissioned), 3).alias("site_commissioned_on_cume_dist"),
        F.round(F.avg("kwh").over(w_site_id_by_site_commissioned_2), 2).alias("kwh_avg_2_prior"),
        F.round(F.cume_dist().over(w_site_id_by_site_commissioned_3), 3).alias("site_commissioned_on_cume_dist_2"),
        F.max("kwh").over(w_site_id).alias("site_id_max_kwh"),
        F.round(F.cume_dist().over(w_site_commissioned_on_month_s_2), 3).alias("kwh_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_commissioned_on_month), 2).alias("kwh_vs_site_commissioned_on_month_avg"),
        F.ntile(3).over(w_site_id_by_site_commissioned_3).alias("site_commissioned_on_tercile"),
        F.min("kwh").over(w_site_commissioned_on_month_b_2).alias("min_kwh_so_far"),
    )
)

first_rows = (
    ranked
    .filter(F.col("kwh_dense_rank") <= 3)
    .drop("kwh_dense_rank")
)

metrics = (
    first_rows
    .select(
        "*",
        F.round(F.avg("site_id_sum_kwh").over(w_kwh_tercile_kwh_quartile), 2).alias("kwh_tercile_kwh_quartile_avg_site_id_sum_kwh"),
        F.rank().over(w_kwh_tercile_by_kwh_ties).alias("kwh_rank"),
        F.dense_rank().over(w_kwh_tercile_by_kwh_ties).alias("kwh_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "kwh",
        "site_commissioned_on",
        "kwh_quartile",
        "site_id_sum_kwh",
        "kwh_tercile",
        "running_kwh",
        "site_commissioned_on_cume_dist",
        "kwh_avg_2_prior",
        "site_id_max_kwh",
        "kwh_cume_dist",
        "kwh_percent_rank",
        "kwh_vs_site_commissioned_on_month_avg",
        "site_commissioned_on_tercile",
        "kwh_tercile_kwh_quartile_avg_site_id_sum_kwh",
        "kwh_rank",
        "kwh_dense_rank",
        F.abs(F.col("kwh_cume_dist") - 0.7).alias("kwh_cume_dist_distance"),
    )
    .orderBy(F.col("site_id_sum_kwh").desc())
)
