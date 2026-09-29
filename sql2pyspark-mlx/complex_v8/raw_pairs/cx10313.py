from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_voltage_full = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id = Window.partitionBy("site_id")
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy("voltage")
w_quality_by_read_at_ties = Window.partitionBy("quality").orderBy(F.col("read_at").desc())
w_site_id_by_voltage_running = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_read_at = Window.partitionBy("quality").orderBy(F.col("read_at").desc(), "id")
w_site_id_quality_by_read_at = Window.partitionBy("site_id", "quality").orderBy("read_at", "id")
w_site_id_by_voltage_prior_2 = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(-2, -1)
w_site_id_by_voltage_prior_1 = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(-1, -1)
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_all_by_voltage_ties = Window.orderBy("voltage")
w_all_by_voltage = Window.orderBy("voltage", "id")

base_readings = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")) & (F.col("site_id") <= 7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

ranked = (
    base_readings
    .select(
        "id",
        "read_at",
        "kwh",
        F.nth_value("kwh", 2).over(w_site_id_by_voltage_full).alias("second_kwh"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.round(F.cume_dist().over(w_site_id_by_voltage_ties), 3).alias("voltage_cume_dist"),
        F.rank().over(w_quality_by_read_at_ties).alias("read_at_rank"),
        F.sum("voltage").over(w_site_id_by_voltage_running).alias("running_voltage"),
        F.ntile(4).over(w_quality_by_read_at).alias("read_at_quartile"),
        F.max("voltage").over(w_site_id).alias("site_id_max_voltage"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
        (F.col("voltage") - F.lag("voltage").over(w_site_id_quality_by_read_at)).alias("voltage_change"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.round(F.avg("voltage").over(w_site_id_by_voltage_prior_2), 2).alias("voltage_avg_2_prior"),
        F.max("kwh").over(w_site_id_by_voltage_prior_1).alias("kwh_max_1_prior"),
        F.round(F.avg("kwh").over(w_site_id_quality), 2).alias("site_id_quality_avg_kwh"),
        F.round(F.percent_rank().over(w_all_by_voltage_ties), 3).alias("voltage_percent_rank"),
        F.lag("kwh", 1, 0).over(w_all_by_voltage).alias("prev_kwh"),
    )
)

enriched = (
    ranked
    .filter(F.col("voltage_cume_dist") >= 0.4)
    .select(
        "id",
        "read_at",
        "kwh",
        "second_kwh",
        "distinct_quality_in_site_id",
        "voltage_cume_dist",
        "running_voltage",
        "read_at_quartile",
        "site_id_max_voltage",
        "voltage_change",
        "rows_in_site_id",
        "kwh_share_of_site_id",
        "voltage_avg_2_prior",
        "kwh_max_1_prior",
        "site_id_quality_avg_kwh",
        "voltage_percent_rank",
        "prev_kwh",
        F.greatest(F.col("site_id_quality_avg_kwh"), F.col("voltage_cume_dist")).alias("greatest_site_id_quality_avg_kwh_voltage_cume_di"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.abs(F.col("site_id_max_voltage") - 994.0).alias("site_id_max_voltage_distance"),
    )
)

result = (
    enriched
    .filter(F.col("site_id_max_voltage_distance").between(857.5, 1960.0))
    .select(
        "id",
        "read_at",
        "kwh",
        "second_kwh",
        "distinct_quality_in_site_id",
        "voltage_cume_dist",
        "running_voltage",
        "read_at_quartile",
        "site_id_max_voltage",
        "kwh_share_of_site_id",
        "voltage_avg_2_prior",
        "kwh_max_1_prior",
        "site_id_quality_avg_kwh",
        "voltage_percent_rank",
        "prev_kwh",
        "greatest_site_id_quality_avg_kwh_voltage_cume_di",
        "site_id_max_voltage_distance",
        F.when(F.col("read_at_quartile") >= 8, "large").when(F.col("read_at_quartile") >= 3, "medium").otherwise("small").alias("read_at_quartile_tier"),
        F.abs(F.col("site_id_max_voltage") - 1778.0).alias("site_id_max_voltage_distance_2"),
    )
    .orderBy(F.col("read_at").desc())
)
