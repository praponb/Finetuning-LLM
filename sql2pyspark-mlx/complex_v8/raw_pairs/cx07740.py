from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_site_id_by_rea = Window.partitionBy(F.month("read_at"), "site_id").orderBy("read_at", "id")
w_prior_rows_3d = Window.partitionBy("prior_rows_3d")
w_read_at_month_site_id_by_rea_2 = Window.partitionBy(F.month("read_at"), "site_id").orderBy("read_at")

joined_ranges = (
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

with_windows = (
    joined_ranges
    .select(
        "*",
        F.lag("prior_rows_3d", 2).over(w_read_at_month_site_id_by_rea).alias("prev2_prior_rows_3d"),
        F.count(F.lit(1)).over(w_prior_rows_3d).alias("rows_in_prior_rows_3d"),
        F.round(F.percent_rank().over(w_read_at_month_site_id_by_rea_2), 3).alias("read_at_percent_rank"),
        F.round(F.col("prior_rows_3d") - F.avg("prior_rows_3d").over(w_prior_rows_3d), 2).alias("prior_rows_3d_vs_prior_rows_3d_avg"),
    )
)

result = (
    with_windows
    .select(
        "id",
        F.col("prev2_prior_rows_3d").alias("prev2_prior_rows_3d_v"),
        F.col("rows_in_prior_rows_3d").alias("rows_in_prior_rows_3d_v"),
        F.col("prior_rows_3d").alias("m_prior_rows_3d"),
    )
    .unpivot(
        ids=["id"],
        values=["prev2_prior_rows_3d_v", "rows_in_prior_rows_3d_v", "m_prior_rows_3d"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric")
)
