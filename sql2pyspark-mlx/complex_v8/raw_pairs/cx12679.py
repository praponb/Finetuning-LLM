from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_12h = Window.partitionBy("prior_rows_12h")
w_all_by_prior_rows_12h_full = Window.orderBy("prior_rows_12h", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_prior_rows_12h_moving = Window.orderBy("prior_rows_12h", "id").rowsBetween(-6, Window.currentRow)

covered = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.voltage").alias("prior_voltage_12h"),
    )
)

with_trends = (
    covered
    .select(
        "*",
        F.count(F.lit(1)).over(w_prior_rows_12h).alias("rows_in_prior_rows_12h"),
        F.nth_value("prior_voltage_12h", 2).over(w_all_by_prior_rows_12h_full).alias("second_prior_voltage_12h"),
        F.round(F.avg("prior_voltage_12h").over(w_all_by_prior_rows_12h_moving), 2).alias("prior_voltage_12h_avg_7"),
    )
)

result = (
    with_trends
    .filter(F.col("rows_in_prior_rows_12h") <= 11)
    .select(
        "id",
        "site_id",
        "prior_rows_12h",
        "prior_voltage_12h",
        "rows_in_prior_rows_12h",
        "second_prior_voltage_12h",
        "prior_voltage_12h_avg_7",
    )
    .orderBy("second_prior_voltage_12h", F.col("rows_in_prior_rows_12h").desc())
)
