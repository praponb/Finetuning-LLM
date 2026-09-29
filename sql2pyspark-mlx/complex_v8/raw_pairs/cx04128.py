from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh_ties = Window.orderBy(F.col("kwh").desc())
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_quality = Window.partitionBy("quality")
w_quality_by_voltage = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id")
w_all_by_kwh = Window.orderBy("kwh", "id")
w_voltage_dense_rank_by_runnin = Window.partitionBy("voltage_dense_rank").orderBy(F.col("running_kwh").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_all_by_greatest_kwh_share_of = Window.orderBy("greatest_kwh_share_of_quality_voltage_rank", "id").rowsBetween(-2, -1)
w_kwh_cume_dist_tier = Window.partitionBy("kwh_cume_dist_tier")

recent_readings = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(10, 14)) | (F.col("quality").isin("BAD", "EST")))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

sequenced = (
    recent_readings
    .select(
        "*",
        F.sum("kwh").over(w_all_by_kwh_running).alias("running_kwh"),
        F.round(F.cume_dist().over(w_all_by_kwh_ties), 3).alias("kwh_cume_dist"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id_quality), 2).alias("voltage_vs_site_id_quality_avg"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id_quality), 2).alias("kwh_vs_site_id_quality_avg"),
        F.rank().over(w_all_by_voltage).alias("voltage_rank"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality), F.lit(0)), 4).alias("kwh_share_of_quality"),
        F.dense_rank().over(w_quality_by_voltage).alias("voltage_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.round(F.avg("voltage").over(w_site_id_quality), 2).alias("site_id_quality_avg_voltage"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_kwh)).alias("voltage_change"),
    )
)

tagged = (
    sequenced
    .select(
        "id",
        "voltage",
        "quality",
        "running_kwh",
        "kwh_cume_dist",
        "voltage_rank",
        "kwh_share_of_quality",
        "voltage_dense_rank",
        "site_id_quality_avg_voltage",
        "voltage_change",
        F.greatest(F.col("kwh_share_of_quality"), F.col("voltage_rank")).alias("greatest_kwh_share_of_quality_voltage_rank"),
        F.when(F.col("kwh_cume_dist") >= 0.8, "large").when(F.col("kwh_cume_dist") >= 0.5, "medium").otherwise("small").alias("kwh_cume_dist_tier"),
    )
)

metrics = (
    tagged
    .select(
        "*",
        F.sum("voltage").over(w_voltage_dense_rank_by_runnin).alias("voltage_sum_6_prior"),
        F.sum("voltage_rank").over(w_all_by_greatest_kwh_share_of).alias("voltage_rank_sum_2_prior"),
        F.max("running_kwh").over(w_kwh_cume_dist_tier).alias("kwh_cume_dist_tier_max_running_kwh"),
        F.sum("voltage_rank").over(w_kwh_cume_dist_tier).alias("kwh_cume_dist_tier_sum_voltage_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "voltage",
        "quality",
        "running_kwh",
        "kwh_cume_dist",
        "voltage_rank",
        "voltage_dense_rank",
        "site_id_quality_avg_voltage",
        "kwh_cume_dist_tier",
        "voltage_rank_sum_2_prior",
        "kwh_cume_dist_tier_max_running_kwh",
    )
    .orderBy("quality", "id")
    .limit(20)
)
