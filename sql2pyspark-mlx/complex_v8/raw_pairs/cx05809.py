from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_by_kwh = Window.partitionBy("zone").orderBy(F.col("kwh").desc(), "id")
w_zone_by_kwh_full = Window.partitionBy("zone").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month = Window.partitionBy(F.date_trunc("month", "read_at"))
w_zone_by_kwh_prior_2 = Window.partitionBy("zone").orderBy(F.col("kwh").desc(), "id").rowsBetween(-2, -1)

matched = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "left",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

windowed = (
    matched
    .select(
        "id",
        "site_id",
        "rate",
        F.rank().over(w_zone_by_kwh).alias("kwh_rank"),
        F.nth_value("kwh", 2).over(w_zone_by_kwh_full).alias("second_kwh"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_month), 2).alias("kwh_vs_read_at_month_avg"),
        F.round(F.avg("kwh").over(w_zone_by_kwh_prior_2), 2).alias("kwh_avg_2_prior"),
    )
)

grouped = (
    windowed
    .groupBy(
        "site_id",
        "kwh_rank",
        F.when(F.col("second_kwh") >= 104.8, "HIGH").when(F.col("second_kwh") >= 37.0, "MEDIUM").otherwise("LOW").alias("second_kwh_tier"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.round(F.stddev("rate"), 2).alias("stddev_rate"),
        F.max("kwh_rank").alias("max_kwh_rank"),
        F.sum("kwh_rank").alias("total_kwh_rank"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    grouped
    .select(
        "site_id",
        "second_kwh_tier",
        "n_rows",
        "stddev_rate",
        "max_kwh_rank",
    )
    .orderBy(F.col("second_kwh_tier").asc())
)
