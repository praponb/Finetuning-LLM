from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_7d_site_id_by_rea = Window.partitionBy("prior_rows_7d", "site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prior_rows_7d_site_id_by_rea_2 = Window.partitionBy("prior_rows_7d", "site_id").orderBy(F.col("read_at").desc())
w_all_by_prior_rows_7d = Window.orderBy(F.col("prior_rows_7d").asc_nulls_last(), "id")

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

metrics = (
    covered
    .select(
        "*",
        F.first("prior_rows_7d").over(w_prior_rows_7d_site_id_by_rea).alias("first_prior_rows_7d"),
        F.round(F.cume_dist().over(w_prior_rows_7d_site_id_by_rea_2), 3).alias("read_at_cume_dist"),
        F.dense_rank().over(w_prior_rows_7d_site_id_by_rea_2).alias("read_at_dense_rank"),
        F.round(F.percent_rank().over(w_prior_rows_7d_site_id_by_rea_2), 3).alias("read_at_percent_rank"),
        F.nth_value("prior_kwh_7d", 2).over(w_prior_rows_7d_site_id_by_rea).alias("second_prior_kwh_7d"),
        F.dense_rank().over(w_all_by_prior_rows_7d).alias("prior_rows_7d_dense_rank"),
    )
)

result = (
    metrics
    .rollup("first_prior_rows_7d", "prior_rows_7d_dense_rank")
    .agg(
        F.grouping_id().alias("gid"),
        F.max("read_at").alias("last_read_at"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("first_prior_rows_7d").asc_nulls_last(),
        F.col("prior_rows_7d_dense_rank").asc_nulls_last(),
    )
)
