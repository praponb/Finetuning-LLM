from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_site_id_by_vol = Window.partitionBy(F.month("read_at"), "site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_month_site_id_by_vol_2 = Window.partitionBy(F.month("read_at"), "site_id").orderBy("voltage", "id").rowsBetween(-2, -1)
w_read_at_month_site_id_by_vol_3 = Window.partitionBy(F.month("read_at"), "site_id").orderBy("voltage", "id")
w_all_by_kwh_bucket_ties = Window.orderBy(F.col("kwh_bucket").desc())
w_site_id = Window.partitionBy("site_id")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy(F.col("read_at").desc_nulls_first(), "id")
w_site_id_by_read_at_moving_2 = Window.partitionBy("site_id").orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_read_at_month_site_id_by_vol_4 = Window.partitionBy(F.month("read_at"), "site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_readings = (
    df_meter_readings
    .filter((F.col("quality").isin("BAD", "EST")) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

ranked = (
    cleaned_readings
    .select(
        "*",
        F.max("kwh").over(w_read_at_month_site_id_by_vol).alias("max_kwh_so_far"),
        F.round(F.avg("kwh").over(w_read_at_month_site_id_by_vol_2), 2).alias("kwh_avg_2_prior"),
        F.round(F.percent_rank().over(w_read_at_month_site_id_by_vol_3), 3).alias("voltage_percent_rank"),
        F.dense_rank().over(w_all_by_kwh_bucket_ties).alias("kwh_bucket_dense_rank"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.row_number().over(w_site_id_by_read_at).alias("seq_no"),
        F.row_number().over(w_read_at_month_site_id_by_vol_3).alias("voltage_seq"),
        F.round(F.col("kwh_bucket") - F.avg("kwh_bucket").over(w_site_id), 2).alias("kwh_bucket_vs_site_id_avg"),
        F.lag("kwh", 2).over(w_site_id_by_read_at).alias("prev2_kwh"),
        F.max("voltage").over(w_site_id_by_read_at_moving_2).alias("voltage_max_3"),
        F.first("read_at").over(w_read_at_month_site_id_by_vol_4).alias("first_read_at"),
        F.ntile(10).over(w_read_at_month_site_id_by_vol_3).alias("voltage_decile"),
    )
)

summary = (
    ranked
    .filter(F.col("site_id").isNotNull())
    .groupBy(
        "voltage_decile",
        F.date_trunc("month", "first_read_at").alias("first_read_at_month"),
        "kwh_bucket_dense_rank",
    )
    .agg(
        F.round(F.sum("voltage_percent_rank") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("voltage_percent_rank_to_kwh_ratio"),
        F.count("*").alias("cnt"),
        F.sum("kwh_bucket_dense_rank").alias("sum_kwh_bucket_dense_rank_check"),
    )
    .filter(F.col("sum_kwh_bucket_dense_rank_check") > 4)
    .drop("sum_kwh_bucket_dense_rank_check")
)

result = (
    summary
    .select(
        "voltage_decile",
        "kwh_bucket_dense_rank",
        "voltage_percent_rank_to_kwh_ratio",
        "cnt",
        F.quarter(F.col("first_read_at_month")).alias("first_read_at_month_quarter"),
    )
    .orderBy(
        F.col("voltage_decile").asc_nulls_last(),
        F.col("kwh_bucket_dense_rank").desc(),
    )
)
