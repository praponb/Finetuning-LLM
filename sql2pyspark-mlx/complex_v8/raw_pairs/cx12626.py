from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh_moving_2 = Window.orderBy("kwh", "id").rowsBetween(-2, Window.currentRow)
w_all_by_kwh = Window.orderBy("kwh", "id")
w_all_by_kwh_full = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh_ties = Window.orderBy("kwh")
w_all_by_voltage_prior_2 = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(-2, -1)

recent_readings = (
    df_meter_readings
    .filter(F.col("site_id") >= 6)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

tagged = (
    recent_readings
    .filter(F.col("quality") != "BAD")
    .select(
        "*",
        F.round(F.col("voltage") * 0.25, 2).alias("voltage_adj"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.round(F.col("voltage") * 1.1, 2).alias("voltage_adj_2"),
    )
)

metrics = (
    tagged
    .select(
        "*",
        F.sum("voltage").over(w_all_by_kwh_moving_2).alias("voltage_sum_3"),
        F.lag("kwh").over(w_all_by_kwh).alias("prev_kwh"),
        F.nth_value("kwh", 2).over(w_all_by_kwh_full).alias("second_kwh"),
        F.rank().over(w_all_by_kwh_ties).alias("kwh_rank"),
        F.max("voltage").over(w_all_by_voltage_prior_2).alias("voltage_max_2_prior"),
    )
)

result = (
    metrics
    .filter(F.col("kwh").between(16.5, 82.9))
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "voltage_adj",
        "quality_slug",
        "voltage_sum_3",
        "prev_kwh",
        "kwh_rank",
    )
    .orderBy(F.col("kwh_rank").desc())
)
