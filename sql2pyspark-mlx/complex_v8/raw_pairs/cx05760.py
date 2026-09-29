from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_all_by_kwh_ties = Window.orderBy("kwh")
w_all_by_kwh_minus_voltage_pri = Window.orderBy(F.col("kwh_minus_voltage").desc(), "id").rowsBetween(-2, -1)

scoped = (
    df_meter_readings
    .filter(F.col("site_id") >= 6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.lead("kwh", 1, 0).over(w_all_by_kwh).alias("next_kwh"),
        F.rank().over(w_all_by_kwh).alias("kwh_rank"),
        F.round(F.cume_dist().over(w_all_by_kwh_ties), 3).alias("kwh_cume_dist"),
        F.lead("kwh_per_voltage", 3, 0).over(w_all_by_kwh).alias("next3_kwh_per_voltage"),
        F.sum("voltage_bucket").over(w_all_by_kwh_minus_voltage_pri).alias("voltage_bucket_sum_2_prior"),
        F.round(F.cume_dist().over(w_all_by_kwh), 3).alias("kwh_cume_dist_2"),
    )
)

derived = (
    metrics
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "kwh",
        "quality",
        "kwh_per_voltage",
        "voltage_bucket",
        "kwh_minus_voltage",
        "next_kwh",
        "kwh_rank",
        "kwh_cume_dist",
        "voltage_bucket_sum_2_prior",
        (F.col("read_at") + F.expr("INTERVAL 15 MINUTES")).alias("read_at_plus_15m"),
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

result = (
    derived
    .cube("kwh_rank", "read_at_year")
    .agg(
        F.sum(F.when(F.col("quality") == "OK", F.col("kwh_per_voltage")).otherwise(0)).alias("kwh_per_voltage_quality_ok"),
    )
    .orderBy(
        F.col("kwh_rank").asc_nulls_last(),
        F.col("read_at_year").asc_nulls_last(),
    )
)
