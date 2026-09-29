from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_kwh_tier_read_at_year_by_vol = Window.partitionBy("kwh_tier", F.year("read_at")).orderBy(F.col("voltage").desc())
w_kwh_tier_by_kwh = Window.partitionBy("kwh_tier").orderBy("kwh", "id")
w_site_id_kwh_tier = Window.partitionBy("site_id", "kwh_tier")
w_all_by_read_at = Window.orderBy("read_at", "id")
w_all_by_read_at_running = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_kwh_tier = Window.partitionBy("kwh_tier")
w_kwh_tier_site_id = Window.partitionBy("kwh_tier", "site_id")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")

cleaned = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) | (F.col("voltage") <= 239.6))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.when(F.col("kwh") >= 78.6, "large").when(F.col("kwh") >= 35.0, "medium").otherwise("small").alias("kwh_tier"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "kwh_tier",
        F.round(F.cume_dist().over(w_kwh_tier_read_at_year_by_vol), 3).alias("voltage_cume_dist"),
        (F.col("kwh") - F.lag("kwh").over(w_kwh_tier_by_kwh)).alias("kwh_change"),
        F.max("voltage").over(w_site_id_kwh_tier).alias("site_id_kwh_tier_max_voltage"),
        F.lag("voltage").over(w_all_by_read_at).alias("prev_voltage"),
        F.sum("kwh").over(w_all_by_read_at_running).alias("running_kwh"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id_kwh_tier), F.lit(0)), 4).alias("voltage_share_of_site_id_kwh_tier"),
        F.lag("kwh", 2).over(w_all_by_kwh).alias("prev2_kwh"),
        F.round(F.avg("voltage").over(w_kwh_tier), 2).alias("kwh_tier_avg_voltage"),
        F.max("voltage").over(w_kwh_tier).alias("kwh_tier_max_voltage"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_kwh_tier), 2).alias("voltage_vs_kwh_tier_avg"),
        F.sum("kwh").over(w_kwh_tier).alias("kwh_tier_sum_kwh"),
        F.round(F.percent_rank().over(w_kwh_tier_by_kwh), 3).alias("kwh_percent_rank"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_kwh_tier_site_id), F.lit(0)), 4).alias("voltage_share_of_kwh_tier_site_id"),
        F.sum("voltage").over(w_kwh_tier_site_id).alias("kwh_tier_site_id_sum_voltage"),
        F.lead("voltage").over(w_site_id_by_read_at).alias("next_voltage"),
    )
)

summary = (
    metrics
    .groupBy(
        "kwh_tier",
        "site_id",
        F.when(F.col("kwh_tier_avg_voltage") >= 236.2, "A").when(F.col("kwh_tier_avg_voltage") >= 225.8, "B").otherwise("C").alias("kwh_tier_avg_voltage_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("kwh_change") / F.nullif(F.sum("running_kwh"), F.lit(0)), 3).alias("kwh_change_to_running_kwh_ratio"),
    )
)

result = (
    summary
    .select(
        "kwh_tier",
        "site_id",
        "row_count",
        "kwh_change_to_running_kwh_ratio",
    )
)
