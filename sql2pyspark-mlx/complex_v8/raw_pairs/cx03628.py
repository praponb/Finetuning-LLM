from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_to_date_site_id = Window.partitionBy(F.to_date("read_at"), "site_id")
w_site_id_site_commissioned_on = Window.partitionBy("site_id", F.date_trunc("month", "site_commissioned_on")).orderBy(F.col("site_commissioned_on").desc(), "id")
w_site_commissioned_on_month_q = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"), "quality").orderBy("read_at")
w_quality = Window.partitionBy("quality")
w_quality_by_read_at_ties = Window.partitionBy("quality").orderBy("read_at")
w_site_id_by_read_at_moving_6 = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(-6, Window.currentRow)
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy("read_at")
w_site_id_site_commissioned_on_2 = Window.partitionBy("site_id", F.date_trunc("month", "site_commissioned_on")).orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(-6, Window.currentRow)
w_site_id_by_read_at_full = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_meter_readings
    .filter((F.col("kwh") >= 29.2) & (F.col("site_id") <= 7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("BAD", "OK"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
    )
)

joined = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

sequenced = (
    joined
    .select(
        "*",
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_to_date_site_id), 2).alias("kwh_vs_read_at_to_date_site_id_avg"),
        F.row_number().over(w_site_id_site_commissioned_on).alias("site_commissioned_on_seq"),
        F.dense_rank().over(w_site_commissioned_on_month_q).alias("read_at_dense_rank"),
        F.min("kwh").over(w_quality).alias("quality_min_kwh"),
        F.dense_rank().over(w_quality_by_read_at_ties).alias("read_at_dense_rank_2"),
        F.max("kwh").over(w_site_id_by_read_at_moving_6).alias("kwh_max_7"),
        F.lag("kwh").over(w_site_id_by_read_at).alias("prev_kwh"),
        F.round(F.cume_dist().over(w_site_commissioned_on_month_q), 3).alias("read_at_cume_dist"),
        F.round(F.cume_dist().over(w_site_id_by_read_at_ties), 3).alias("read_at_cume_dist_2"),
        F.round(F.percent_rank().over(w_quality_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.round(F.avg("kwh").over(w_site_id_site_commissioned_on_2), 2).alias("kwh_avg_7"),
        F.count(F.lit(1)).over(w_read_at_to_date_site_id).alias("rows_in_read_at_to_date_site_id"),
        F.rank().over(w_quality_by_read_at_ties).alias("read_at_rank"),
        F.last("kwh").over(w_site_id_by_read_at_full).alias("last_kwh"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "site_commissioned_on",
        "site_commissioned_on_seq",
        "quality_min_kwh",
        "read_at_dense_rank_2",
        "kwh_max_7",
        "read_at_cume_dist",
        "read_at_percent_rank",
        "kwh_avg_7",
        "rows_in_read_at_to_date_site_id",
        "last_kwh",
        F.date_trunc("quarter", F.col("read_at")).alias("read_at_quarter"),
    )
)
