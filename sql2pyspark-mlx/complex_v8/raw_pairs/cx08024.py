from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_kwh_moving_1 = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_kwh_prior_6 = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(-6, -1)

prepared = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
    )
)

with_trends = (
    prepared
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "least_voltage_kwh",
        F.round(F.col("least_voltage_kwh") / F.nullif(F.sum("least_voltage_kwh").over(w_quality_site_id), F.lit(0)), 4).alias("least_voltage_kwh_share_of_quality_site_id"),
        F.sum("least_voltage_kwh").over(w_all_by_kwh_moving_1).alias("least_voltage_kwh_sum_2"),
        F.round(F.avg("kwh").over(w_all_by_kwh_prior_6), 2).alias("kwh_avg_6_prior"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["quality", "site_id"], ["quality"], []],
        "quality", "site_id",
    )
    .agg(
        F.sum(F.when(F.col("quality").isin("BAD", "OK"), F.col("least_voltage_kwh_share_of_quality_site_id")).otherwise(0)).alias("least_voltage_kwh_share_of_quality_site_id_quali"),
        F.sum(F.when(F.col("quality") == "EST", F.col("least_voltage_kwh"))).alias("least_voltage_kwh_quality_est"),
        F.min("kwh_avg_6_prior").alias("min_kwh_avg_6_prior"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "site_id",
        "least_voltage_kwh_share_of_quality_site_id_quali",
        "least_voltage_kwh_quality_est",
        "min_kwh_avg_6_prior",
    )
    .orderBy(F.col("quality").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
