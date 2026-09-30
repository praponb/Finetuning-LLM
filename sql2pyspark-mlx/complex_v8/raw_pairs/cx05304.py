from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_read_at = Window.partitionBy("quality", "site_id").orderBy("read_at", "id")
w_read_at_month_site_id = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id")
w_site_id = Window.partitionBy("site_id")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id")
w_all_by_voltage_prior_2 = Window.orderBy("voltage", "id").rowsBetween(-2, -1)
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id")
w_all_by_read_at_full = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_read_at_rank = Window.orderBy(F.col("read_at_rank").desc(), "id")
w_voltage_tier_site_id = Window.partitionBy("voltage_tier", "site_id")
w_read_at_month_read_at_rank = Window.partitionBy(F.month("read_at"), "read_at_rank")
w_all_by_read_at_month_site_id = Window.orderBy(F.col("read_at_month_site_id_sum_voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_readings = (
    df_meter_readings
    .filter((F.col("kwh").between(33.9, 117.4)) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

combined_details = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "si.site_name",
    )
)

with_trends = (
    combined_details
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "site_name",
        F.rank().over(w_quality_site_id_by_read_at).alias("read_at_rank"),
        F.min("voltage").over(w_read_at_month_site_id).alias("read_at_month_site_id_min_voltage"),
        F.sum("voltage").over(w_site_id).alias("site_id_sum_voltage"),
        F.min("voltage").over(w_site_id).alias("site_id_min_voltage"),
        (F.col("voltage") - F.lag("voltage").over(w_site_id_by_read_at)).alias("voltage_change"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_read_at_month_site_id), F.lit(0)), 4).alias("voltage_share_of_read_at_month_site_id"),
        F.sum("voltage").over(w_read_at_month_site_id).alias("read_at_month_site_id_sum_voltage"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_read_at_month_site_id), 2).alias("voltage_vs_read_at_month_site_id_avg"),
        F.sum("voltage").over(w_all_by_voltage_prior_2).alias("voltage_sum_2_prior"),
        F.row_number().over(w_site_id_by_voltage).alias("seq_no"),
        F.last("read_at").over(w_all_by_read_at_full).alias("last_read_at"),
    )
)

enriched = (
    with_trends
    .filter(F.upper(F.trim(F.col("site_name"))).like("S%"))
    .select(
        "*",
        F.when(F.col("voltage") >= 231.7, "large").when(F.col("voltage") >= 218.5, "medium").otherwise("small").alias("voltage_tier"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.lead("voltage_filled", 2).over(w_all_by_read_at_rank).alias("next2_voltage_filled"),
        F.sum("site_id_sum_voltage").over(w_voltage_tier_site_id).alias("voltage_tier_site_id_sum_site_id_sum_voltage"),
        F.round(F.col("voltage_sum_2_prior") - F.avg("voltage_sum_2_prior").over(w_read_at_month_read_at_rank), 2).alias("voltage_sum_2_prior_vs_read_at_month_read_at_ran"),
        F.round(F.avg("voltage_filled").over(w_voltage_tier_site_id), 2).alias("voltage_tier_site_id_avg_voltage_filled"),
        F.round(F.col("read_at_rank") / F.nullif(F.sum("read_at_rank").over(w_voltage_tier_site_id), F.lit(0)), 4).alias("read_at_rank_share_of_voltage_tier_site_id"),
        F.last("voltage_filled", ignorenulls=True).over(w_all_by_read_at_month_site_id).alias("last_known_voltage_filled"),
    )
)

result = (
    metrics
    .select(
        "id",
        "site_id",
        "voltage",
        "site_name",
        "read_at_rank",
        "read_at_month_site_id_min_voltage",
        "site_id_min_voltage",
        "voltage_change",
        "voltage_share_of_read_at_month_site_id",
        "voltage_vs_read_at_month_site_id_avg",
        "voltage_sum_2_prior",
        "last_read_at",
        "voltage_tier",
        "voltage_filled",
        "next2_voltage_filled",
        "voltage_tier_site_id_sum_site_id_sum_voltage",
        "voltage_tier_site_id_avg_voltage_filled",
        "read_at_rank_share_of_voltage_tier_site_id",
        F.concat(F.col("voltage_tier"), F.lit("#"), F.col("read_at_rank").cast("string")).alias("voltage_tier_ref"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_read_at")).alias("days_since_last_read_at"),
    )
    .orderBy(F.col("voltage_tier").desc_nulls_first(), F.col("id").desc())
)
