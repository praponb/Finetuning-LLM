from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality = Window.partitionBy("site_id", "quality")
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")
w_site_id_read_at_to_date_by_v = Window.partitionBy("site_id", F.to_date("read_at")).orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_all_by_kwh_prior_3 = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(-3, -1)
w_site_id_quality_by_days_sinc = Window.partitionBy("site_id", "quality").orderBy(F.col("days_since_read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_read_at_to_date_by_v_2 = Window.partitionBy("site_id", F.to_date("read_at")).orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id = Window.partitionBy("site_id")
w_quality_by_voltage = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id")
w_all_by_kwh_full = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_read_at_to_date_by_v_3 = Window.partitionBy("site_id", F.to_date("read_at")).orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_voltage_running = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_voltage_seq_by_days_since_fi = Window.partitionBy("voltage_seq").orderBy(F.col("days_since_first_read_at_to_last_voltage_2_ratio").desc(), "first_read_at_year", "read_at_month", "voltage_seq")
w_first_read_at_year = Window.partitionBy("first_read_at_year")

valid = (
    df_meter_readings
    .filter(F.col("quality") == "EST")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

sequenced = (
    valid
    .select(
        "*",
        F.min("days_since_read_at").over(w_site_id_quality).alias("site_id_quality_min_days_since_read_at"),
        F.round(F.cume_dist().over(w_all_by_voltage), 3).alias("voltage_cume_dist"),
        F.round(F.avg("days_since_read_at").over(w_site_id_read_at_to_date_by_v), 2).alias("days_since_read_at_avg_3_prior"),
        F.dense_rank().over(w_all_by_voltage).alias("voltage_dense_rank"),
        F.max("days_since_read_at").over(w_all_by_kwh_prior_3).alias("days_since_read_at_max_3_prior"),
        F.last("voltage").over(w_site_id_quality_by_days_sinc).alias("last_voltage"),
        F.first("read_at").over(w_site_id_read_at_to_date_by_v_2).alias("first_read_at"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.row_number().over(w_quality_by_voltage).alias("voltage_seq"),
        F.last("voltage").over(w_all_by_kwh_full).alias("last_voltage_2"),
        F.max("days_since_read_at").over(w_site_id_read_at_to_date_by_v_3).alias("max_days_since_read_at_so_far"),
        F.ntile(5).over(w_all_by_voltage).alias("voltage_quintile"),
        F.first("read_at", ignorenulls=True).over(w_quality_by_voltage_running).alias("first_known_read_at"),
    )
)

labeled = (
    sequenced
    .filter(F.col("rows_in_site_id") <= 17)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "days_since_read_at",
        "site_id_quality_min_days_since_read_at",
        "days_since_read_at_avg_3_prior",
        "days_since_read_at_max_3_prior",
        "last_voltage",
        "first_read_at",
        "voltage_seq",
        "last_voltage_2",
        "max_days_since_read_at_so_far",
        "voltage_quintile",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("first_read_at")).alias("days_since_first_read_at"),
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.when(F.col("quality").isin("EST", "OK"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
)

grouped = (
    labeled
    .groupBy(
        F.year("first_read_at").alias("first_read_at_year"),
        F.date_trunc("month", "read_at").alias("read_at_month"),
        "voltage_seq",
    )
    .agg(
        F.round(F.sum("days_since_first_read_at") / F.nullif(F.sum("last_voltage_2"), F.lit(0)), 3).alias("days_since_first_read_at_to_last_voltage_2_ratio"),
        F.round(F.avg("last_voltage_2"), 2).alias("avg_last_voltage_2"),
    )
)

ranked = (
    grouped
    .select(
        "first_read_at_year",
        "read_at_month",
        "voltage_seq",
        "days_since_first_read_at_to_last_voltage_2_ratio",
        "avg_last_voltage_2",
        F.round(F.percent_rank().over(w_voltage_seq_by_days_since_fi), 3).alias("days_since_first_read_at_to_last_voltage_2_ratio_2"),
        F.min("first_read_at_year").over(w_first_read_at_year).alias("first_read_at_year_min_first_read_at_year"),
    )
)

result = (
    ranked
    .select(
        "first_read_at_year",
        "read_at_month",
        "voltage_seq",
        "days_since_first_read_at_to_last_voltage_2_ratio",
        "avg_last_voltage_2",
        "days_since_first_read_at_to_last_voltage_2_ratio_2",
        "first_read_at_year_min_first_read_at_year",
        F.abs(F.col("avg_last_voltage_2") - 231.0).alias("avg_last_voltage_2_distance"),
        F.when(F.col("avg_last_voltage_2") >= 231.0, "large").when(F.col("avg_last_voltage_2") >= 224.0, "medium").otherwise("small").alias("avg_last_voltage_2_tier"),
    )
    .orderBy(
        "days_since_first_read_at_to_last_voltage_2_ratio_2",
        F.col("read_at_month").asc_nulls_last(),
        "first_read_at_year",
        "voltage_seq",
    )
    .limit(5)
)
