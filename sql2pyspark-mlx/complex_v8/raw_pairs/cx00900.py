from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("prior_rows_7d").desc(), F.col("id").desc())
w_site_id_prior_rows_7d_by_pri = Window.partitionBy("site_id", "prior_rows_7d").orderBy("prior_voltage_7d", "id")

in_window = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.voltage").alias("prior_voltage_7d"),
    )
)

latest_per_key = (
    in_window
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows = (
    latest_per_key
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_voltage_7d",
        F.ntile(4).over(w_site_id_prior_rows_7d_by_pri).alias("prior_voltage_7d_quartile"),
        F.ntile(5).over(w_site_id_prior_rows_7d_by_pri).alias("prior_voltage_7d_quintile"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_voltage_7d",
        "prior_voltage_7d_quartile",
        F.round(F.col("prior_voltage_7d") * 1.5, 2).alias("prior_voltage_7d_adj"),
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
    )
    .orderBy(F.col("site_id").desc_nulls_first())
)
