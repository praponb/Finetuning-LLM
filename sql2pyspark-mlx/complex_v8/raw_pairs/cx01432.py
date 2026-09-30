from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_7d_site_id_by_rea = Window.partitionBy("prior_rows_7d", "site_id").orderBy("read_at", "id").rowsBetween(-6, Window.currentRow)
w_site_id = Window.partitionBy("site_id")
w_prior_rows_7d_site_id_by_rea_2 = Window.partitionBy("prior_rows_7d", "site_id").orderBy("read_at", "id")
w_all_by_rows_in_site_id = Window.orderBy("rows_in_site_id", "id")
w_all_by_minutes_since_prev_re = Window.orderBy("minutes_since_prev_read_at", "id")

matched = (
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
        F.sum("b.kwh").alias("prior_kwh_7d"),
    )
)

with_activity = (
    matched
    .crossJoin(matched.agg(F.avg("prior_kwh_7d").alias("overall_avg_prior_kwh_7d")))
    .filter(F.col("prior_kwh_7d") > F.col("overall_avg_prior_kwh_7d"))
    .drop("overall_avg_prior_kwh_7d")
)

windowed = (
    with_activity
    .select(
        "id",
        "read_at",
        "prior_kwh_7d",
        F.round(F.avg("prior_kwh_7d").over(w_prior_rows_7d_site_id_by_rea), 2).alias("prior_kwh_7d_avg_7"),
        F.round(F.col("prior_rows_7d") - F.avg("prior_rows_7d").over(w_site_id), 2).alias("prior_rows_7d_vs_site_id_avg"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_prior_rows_7d_site_id_by_rea_2))) / 60, 1).alias("minutes_since_prev_read_at"),
    )
)

metrics = (
    windowed
    .select(
        "*",
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_rows_in_site_id))) / 60, 1).alias("minutes_since_prev_read_at_2"),
        F.lag("read_at").over(w_all_by_rows_in_site_id).alias("prev_read_at"),
        F.round(F.percent_rank().over(w_all_by_minutes_since_prev_re), 3).alias("minutes_since_prev_read_at_percent_rank"),
        (F.col("rows_in_site_id") - F.lag("rows_in_site_id").over(w_all_by_minutes_since_prev_re)).alias("rows_in_site_id_change"),
    )
)

result = (
    metrics
    .select(
        "id",
        "prior_kwh_7d_avg_7",
        "prior_kwh_7d",
        "minutes_since_prev_read_at",
        "minutes_since_prev_read_at_2",
    )
    .unpivot(
        ids=["id"],
        values=["prior_kwh_7d_avg_7", "prior_kwh_7d", "minutes_since_prev_read_at", "minutes_since_prev_read_at_2"],
        variableColumnName="field",
        valueColumnName="value",
    )
)
