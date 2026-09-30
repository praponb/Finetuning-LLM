from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_site_id_quality_by_voltage_t = Window.partitionBy("site_id", "quality").orderBy("voltage")
w_site_id_quality_by_read_at = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").desc(), "id")
w_quality = Window.partitionBy("quality")
w_read_at_year = Window.partitionBy(F.year("read_at"))
w_all_by_kwh_moving_6 = Window.orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_kwh = Window.orderBy(F.col("kwh").desc_nulls_first(), "id")
w_site_id_quality_by_read_at_t = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").desc())
w_quality_read_at_day_month_by = Window.partitionBy("quality", F.date_trunc("month", "read_at_day")).orderBy("n_quality_is_not_null", "quality", "read_at_day", "next_read_at_ym").rowsBetween(-6, -1)

valid = (
    df_meter_readings
    .filter((F.col("voltage") <= 229.1) & (F.col("read_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

metrics = (
    valid
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.round(F.percent_rank().over(w_all_by_read_at), 3).alias("read_at_percent_rank"),
        F.round(F.percent_rank().over(w_site_id_quality_by_voltage_t), 3).alias("voltage_percent_rank"),
        F.round(F.cume_dist().over(w_site_id_quality_by_read_at), 3).alias("read_at_cume_dist"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality), 2).alias("kwh_vs_quality_avg"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_year), F.lit(0)), 4).alias("kwh_share_of_read_at_year"),
        F.max("voltage").over(w_all_by_kwh_moving_6).alias("voltage_max_7"),
        F.lead("read_at").over(w_all_by_kwh).alias("next_read_at"),
        F.round(F.avg("voltage").over(w_read_at_year), 2).alias("read_at_year_avg_voltage"),
        F.round(F.cume_dist().over(w_site_id_quality_by_read_at_t), 3).alias("read_at_cume_dist_2"),
    )
)

derived = (
    metrics
    .withColumn("quality_ref", F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")))
)

summary = (
    derived
    .groupBy(
        "quality",
        F.to_date("read_at").alias("read_at_day"),
        F.date_format("next_read_at", "yyyy-MM").alias("next_read_at_ym"),
    )
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("n_quality_is_not_null"),
    )
)

with_windows = (
    summary
    .withColumn("quality_max_n_quality_is_not_null", F.max("n_quality_is_not_null").over(w_quality))
    .withColumn("n_quality_is_not_null_avg_6_prior", F.round(F.avg("n_quality_is_not_null").over(w_quality_read_at_day_month_by), 2))
)

result = (
    with_windows
    .select(
        "quality",
        "read_at_day",
        "n_quality_is_not_null",
        "quality_max_n_quality_is_not_null",
        F.coalesce(F.col("n_quality_is_not_null"), F.lit(0)).alias("n_quality_is_not_null_filled"),
    )
)
