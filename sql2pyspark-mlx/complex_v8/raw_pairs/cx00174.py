from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_kwh = Window.orderBy("kwh", "id")
w_all_by_kwh_ties = Window.orderBy("kwh")
w_all_by_kwh_moving_6 = Window.orderBy("kwh", "id").rowsBetween(-6, Window.currentRow)
w_quality = Window.partitionBy("quality")
w_quality_site_id_by_kwh_runni = Window.partitionBy("quality", "site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_quality_site_id_min_v = Window.orderBy("quality_site_id_min_voltage_minus_kwh", "id")

scoped = (
    df_meter_readings
    .filter(F.col("voltage") > 223.3)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.min("voltage_minus_kwh").over(w_quality_site_id).alias("quality_site_id_min_voltage_minus_kwh"),
        F.lag("kwh").over(w_all_by_kwh).alias("prev_kwh"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_site_id), F.lit(0)), 4).alias("kwh_share_of_quality_site_id"),
        F.round(F.percent_rank().over(w_all_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.sum("voltage_minus_kwh").over(w_all_by_kwh_moving_6).alias("voltage_minus_kwh_sum_7"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.round(F.avg("kwh_percent_rank").over(w_quality), 2).alias("quality_avg_kwh_percent_rank"),
        F.first("voltage_minus_kwh", ignorenulls=True).over(w_quality_site_id_by_kwh_runni).alias("first_known_voltage_minus_kwh"),
        F.round(F.cume_dist().over(w_all_by_quality_site_id_min_v), 3).alias("quality_site_id_min_voltage_minus_kwh_cume_dist"),
    )
)

result = (
    metrics
    .rollup("site_id", "quality")
    .agg(F.countDistinct("site_id").alias("distinct_site_ids"))
    .orderBy(F.col("site_id").asc_nulls_last(), F.col("quality").asc_nulls_last())
)
