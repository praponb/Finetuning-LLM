from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_prior_rows_1d = Window.partitionBy("site_id", "prior_rows_1d")
w_prior_rows_1d_by_prior_rows = Window.partitionBy("prior_rows_1d").orderBy("prior_rows_1d")
w_all_by_prior_voltage_1d = Window.orderBy("prior_voltage_1d", "id")

matched = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.voltage").alias("prior_voltage_1d"),
    )
)

with_details_site = (
    matched.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.prior_rows_1d",
        "s.prior_voltage_1d",
        F.col("si.tariff").alias("site_tariff"),
    )
)

with_trends = (
    with_details_site
    .select(
        "id",
        "site_id",
        "prior_rows_1d",
        "prior_voltage_1d",
        F.sum("prior_rows_1d").over(w_site_id_prior_rows_1d).alias("site_id_prior_rows_1d_sum_prior_rows_1d"),
        F.round(F.percent_rank().over(w_prior_rows_1d_by_prior_rows), 3).alias("prior_rows_1d_percent_rank"),
        F.dense_rank().over(w_prior_rows_1d_by_prior_rows).alias("prior_rows_1d_dense_rank"),
        F.dense_rank().over(w_all_by_prior_voltage_1d).alias("prior_voltage_1d_dense_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("prior_rows_1d") > 1)
    .select("prior_rows_1d", "prior_rows_1d_dense_rank")
    .distinct()
    .orderBy("prior_rows_1d")
)
