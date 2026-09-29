from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage_filled_prior = Window.orderBy(F.col("voltage_filled").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_all_by_kwh_adj_running = Window.orderBy("kwh_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_voltage_filled_max_2 = Window.orderBy(F.col("voltage_filled_max_2_prior").asc_nulls_last())
w_all_by_voltage_filled_max_2_2 = Window.orderBy(F.col("voltage_filled_max_2_prior").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_voltage_filled_max_2_3 = Window.orderBy(F.col("voltage_filled_max_2_prior").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage").between(220.2, 239.6)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
        F.greatest(F.col("voltage"), F.col("kwh")).alias("greatest_voltage_kwh"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

derived = (
    scoped_readings
    .filter(F.col("kwh") <= 104.8)
    .select(
        "id",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        F.round(F.col("kwh") * 0.25, 2).alias("kwh_adj"),
        F.substring(F.trim(F.col("quality_clean")), 1, 4).alias("quality_clean_prefix"),
    )
)

screened = (
    derived
    .crossJoin(derived.agg(F.max("voltage_filled").alias("overall_max_voltage_filled")))
    .filter(F.col("voltage_filled") < F.col("overall_max_voltage_filled"))
    .drop("overall_max_voltage_filled")
)

ranked = (
    screened
    .select(
        "*",
        F.max("voltage_filled").over(w_all_by_voltage_filled_prior).alias("voltage_filled_max_2_prior"),
        F.first("voltage_filled", ignorenulls=True).over(w_all_by_kwh_adj_running).alias("first_known_voltage_filled"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.dense_rank().over(w_all_by_voltage_filled_max_2).alias("voltage_filled_max_2_prior_dense_rank"),
        F.sum("first_known_voltage_filled").over(w_all_by_voltage_filled_max_2_2).alias("first_known_voltage_filled_sum_3"),
        F.sum("kwh_adj").over(w_all_by_voltage_filled_max_2_3).alias("running_kwh_adj"),
        F.first("voltage_filled", ignorenulls=True).over(w_all_by_voltage_filled_max_2_3).alias("first_known_voltage_filled_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "voltage_filled",
        "quality_clean_prefix",
        "voltage_filled_max_2_prior",
        "voltage_filled_max_2_prior_dense_rank",
        "first_known_voltage_filled_sum_3",
        "first_known_voltage_filled_2",
    )
    .orderBy(F.col("voltage_filled_max_2_prior").asc_nulls_last())
)
