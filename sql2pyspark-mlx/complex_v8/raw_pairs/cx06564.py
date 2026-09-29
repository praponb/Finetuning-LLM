from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_7d_site_id = Window.partitionBy("prior_rows_7d", "site_id")
w_prior_rows_7d_by_prior_kwh_7 = Window.partitionBy("prior_rows_7d").orderBy(F.col("prior_kwh_7d").desc(), "id")
w_prior_rows_7d_by_prior_kwh_7_2 = Window.partitionBy("prior_rows_7d").orderBy(F.col("prior_kwh_7d").desc())

covered = (
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

sequenced = (
    covered
    .select(
        "id",
        "site_id",
        "prior_rows_7d",
        F.count(F.lit(1)).over(w_prior_rows_7d_site_id).alias("rows_in_prior_rows_7d_site_id"),
        (F.col("prior_rows_7d") - F.lag("prior_rows_7d").over(w_prior_rows_7d_by_prior_kwh_7)).alias("prior_rows_7d_change"),
        F.round(F.avg("prior_rows_7d").over(w_prior_rows_7d_site_id), 2).alias("prior_rows_7d_site_id_avg_prior_rows_7d"),
        F.dense_rank().over(w_prior_rows_7d_by_prior_kwh_7_2).alias("prior_kwh_7d_dense_rank"),
    )
)

screened = (
    sequenced.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
    .filter(F.col("prior_rows_7d_site_id_avg_prior_rows_7d") >= 3.6)
)

result = (
    screened
    .filter(F.col("rows_in_prior_rows_7d_site_id").between(4, 14))
    .select(
        "id",
        "site_id",
        "prior_rows_7d_site_id_avg_prior_rows_7d",
        "prior_kwh_7d_dense_rank",
        F.abs(F.col("prior_rows_7d_change") - -2).alias("prior_rows_7d_change_distance"),
    )
    .orderBy(F.col("prior_kwh_7d_dense_rank").desc(), "id")
    .limit(10)
)
