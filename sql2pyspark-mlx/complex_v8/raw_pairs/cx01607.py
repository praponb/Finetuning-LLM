from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_3d_site_id = Window.partitionBy("prior_rows_3d", "site_id")
w_prior_rows_3d_by_read_at_tie = Window.partitionBy("prior_rows_3d").orderBy("read_at")
w_prior_rows_3d_by_read_at = Window.partitionBy("prior_rows_3d").orderBy("read_at", "id")
w_prior_rows_3d_by_read_at_mov = Window.partitionBy("prior_rows_3d").orderBy("read_at", "id").rowsBetween(-3, Window.currentRow)
w_prior_rows_3d_by_read_at_pri = Window.partitionBy("prior_rows_3d").orderBy("read_at", "id").rowsBetween(-2, -1)

in_window = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.voltage").alias("prior_voltage_3d"),
    )
)

with_trends = (
    in_window
    .select(
        "*",
        F.sum("prior_rows_3d").over(w_prior_rows_3d_site_id).alias("prior_rows_3d_site_id_sum_prior_rows_3d"),
        F.round(F.percent_rank().over(w_prior_rows_3d_by_read_at_tie), 3).alias("read_at_percent_rank"),
        F.lead("prior_rows_3d").over(w_prior_rows_3d_by_read_at).alias("next_prior_rows_3d"),
        F.round(F.avg("prior_voltage_3d").over(w_prior_rows_3d_site_id), 2).alias("prior_rows_3d_site_id_avg_prior_voltage_3d"),
        F.round(F.avg("prior_rows_3d").over(w_prior_rows_3d_by_read_at_mov), 2).alias("prior_rows_3d_avg_4"),
        F.round(F.avg("prior_voltage_3d").over(w_prior_rows_3d_by_read_at_pri), 2).alias("prior_voltage_3d_avg_2_prior"),
    )
)

scored = (
    with_trends
    .filter(F.col("next_prior_rows_3d") < 5)
    .select(
        "*",
        F.round(F.col("read_at_percent_rank") * 0.9, 2).alias("read_at_percent_rank_adj"),
        (F.floor(F.col("prior_voltage_3d") / 250) * 250).alias("prior_voltage_3d_bucket"),
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
    )
)

result = (
    scored
    .select(
        "id",
        "read_at",
        "prior_rows_3d",
        "prior_rows_3d_site_id_sum_prior_rows_3d",
        "read_at_percent_rank",
        "next_prior_rows_3d",
        "prior_rows_3d_avg_4",
        "read_at_percent_rank_adj",
        "prior_voltage_3d_bucket",
        "read_at_quarter",
    )
    .orderBy(
        F.col("prior_voltage_3d_bucket").desc(),
        F.col("read_at_quarter").desc(),
        "id",
    )
    .limit(10)
)
