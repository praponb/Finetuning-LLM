from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_by_read_at_full = Window.partitionBy("read_at_year").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_year_site_id = Window.partitionBy(F.year("read_at"), "site_id")
w_site_id_by_read_at_year = Window.partitionBy("site_id").orderBy(F.col("read_at_year").desc(), "id")
w_site_id_by_kwh_ties = Window.partitionBy("site_id").orderBy(F.col("kwh").desc_nulls_first())
w_site_id_read_at_year_by_read = Window.partitionBy("site_id", F.year("read_at")).orderBy(F.col("read_at").desc())
w_site_id_read_at_year_by_read_2 = Window.partitionBy("site_id", F.year("read_at")).orderBy(F.col("read_at").desc(), "id")
w_read_at_year = Window.partitionBy("read_at_year")
w_kwh_dense_rank_read_at_year = Window.partitionBy("kwh_dense_rank", "read_at_year").orderBy(F.col("stddev_voltage").desc(), "read_at_year", "kwh_dense_rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_readings = (
    df_meter_readings
    .filter(F.month(F.col("read_at")).isin(1, 2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

sequenced = (
    cleaned_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "read_at_year",
        F.nth_value("read_at_year", 2).over(w_read_at_year_by_read_at_full).alias("second_read_at_year"),
        F.count(F.lit(1)).over(w_read_at_year_site_id).alias("rows_in_read_at_year_site_id"),
        F.round(F.avg("voltage").over(w_read_at_year_site_id), 2).alias("read_at_year_site_id_avg_voltage"),
        F.lead("read_at_year").over(w_site_id_by_read_at_year).alias("next_read_at_year"),
        F.round(F.avg("kwh").over(w_read_at_year_site_id), 2).alias("read_at_year_site_id_avg_kwh"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_read_at_year_site_id), F.lit(0)), 4).alias("voltage_share_of_read_at_year_site_id"),
        F.max("voltage").over(w_read_at_year_site_id).alias("read_at_year_site_id_max_voltage"),
        F.rank().over(w_site_id_by_read_at_year).alias("read_at_year_rank"),
        F.dense_rank().over(w_site_id_by_kwh_ties).alias("kwh_dense_rank"),
        F.round(F.cume_dist().over(w_site_id_read_at_year_by_read), 3).alias("read_at_cume_dist"),
        F.sum("voltage").over(w_read_at_year_site_id).alias("read_at_year_site_id_sum_voltage"),
        F.ntile(10).over(w_site_id_read_at_year_by_read_2).alias("read_at_decile"),
    )
)

stats = (
    sequenced
    .groupBy("read_at_year", "kwh_dense_rank")
    .agg(
        F.round(F.stddev("voltage"), 2).alias("stddev_voltage"),
        F.percentile_approx("voltage", 0.5).alias("median_voltage"),
    )
)

ranked = (
    stats
    .select(
        "*",
        F.round(F.avg("kwh_dense_rank").over(w_read_at_year), 2).alias("read_at_year_avg_kwh_dense_rank"),
        F.min("kwh_dense_rank").over(w_kwh_dense_rank_read_at_year).alias("min_kwh_dense_rank_so_far"),
    )
)

result = (
    ranked
    .select(
        "read_at_year",
        "kwh_dense_rank",
        "median_voltage",
        "min_kwh_dense_rank_so_far",
    )
    .orderBy(F.col("median_voltage").desc(), F.col("read_at_year").asc_nulls_last())
)
