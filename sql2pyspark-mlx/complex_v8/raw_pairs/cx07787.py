from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_quality = Window.partitionBy("quality")
w_all_by_read_at_running = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_to_date_by_voltage_t = Window.partitionBy(F.to_date("read_at")).orderBy(F.col("voltage").desc())
w_read_at_to_date_by_voltage = Window.partitionBy(F.to_date("read_at")).orderBy(F.col("voltage").desc(), "id")
w_kwh_tier_site_id_by_kwh_prio = Window.partitionBy("kwh_tier", "site_id").orderBy("kwh", "id").rowsBetween(-6, -1)
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_meter_readings
    .filter(F.col("voltage").between(213.5, 235.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 83.8, "A").when(F.col("kwh") >= 50.4, "B").otherwise("C").alias("kwh_tier"),
    )
)

with_windows = (
    scoped
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.row_number().over(w_all_by_read_at).alias("read_at_seq"),
        F.sum("voltage").over(w_quality).alias("quality_sum_voltage"),
        F.first("voltage", ignorenulls=True).over(w_all_by_read_at_running).alias("first_known_voltage"),
        F.rank().over(w_read_at_to_date_by_voltage_t).alias("voltage_rank"),
        F.lead("read_at", 2).over(w_read_at_to_date_by_voltage).alias("next2_read_at"),
        (F.col("voltage") - F.lag("voltage").over(w_read_at_to_date_by_voltage)).alias("voltage_change"),
        F.round(F.avg("kwh").over(w_kwh_tier_site_id_by_kwh_prio), 2).alias("kwh_avg_6_prior"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality), 2).alias("voltage_vs_quality_avg"),
        F.min("kwh").over(w_all_by_kwh_running).alias("min_kwh_so_far"),
    )
)

top_rows = (
    with_windows
    .filter(F.col("voltage_rank") == 1)
    .drop("voltage_rank")
)

per_group = (
    top_rows
    .groupBy(
        F.when(F.col("kwh") >= 82.9, "large").when(F.col("kwh") >= 33.9, "medium").otherwise("small").alias("kwh_tier"),
        F.year("next2_read_at").alias("next2_read_at_year"),
    )
    .agg(
        F.max("kwh").alias("max_kwh"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("next2_read_at").alias("last_next2_read_at"),
        F.round(F.sum("first_known_voltage") / F.nullif(F.sum("kwh_avg_6_prior"), F.lit(0)), 3).alias("first_known_voltage_to_kwh_avg_6_prior_ratio"),
        F.percentile_approx("voltage_change", 0.5).alias("median_voltage_change"),
        F.sum("voltage").alias("sum_voltage_check"),
    )
    .filter(F.col("sum_voltage_check") > 219.6)
    .drop("sum_voltage_check")
)

result = (
    per_group
    .select(
        "kwh_tier",
        "next2_read_at_year",
        "last_next2_read_at",
        "median_voltage_change",
        F.coalesce(F.col("kwh_tier"), F.lit("UNKNOWN")).alias("kwh_tier_or_unknown"),
        F.length(F.trim(F.col("kwh_tier"))).alias("kwh_tier_len"),
    )
    .orderBy(
        F.col("kwh_tier_len").desc(),
        F.col("next2_read_at_year").asc_nulls_last(),
    )
)
