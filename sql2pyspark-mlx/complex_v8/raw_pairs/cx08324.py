from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_voltage_prior_1 = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id").rowsBetween(-1, -1)
w_all_by_read_at = Window.orderBy(F.col("read_at").asc_nulls_last(), "id")
w_site_id = Window.partitionBy("site_id")
w_quality_by_voltage_ties = Window.partitionBy("quality").orderBy(F.col("voltage").desc())
w_read_at_dense_rank_site_id_b = Window.partitionBy("read_at_dense_rank", "site_id").orderBy("voltage")
w_read_at_dense_rank_site_id_b_2 = Window.partitionBy("read_at_dense_rank", "site_id").orderBy("voltage", "id").rowsBetween(-3, Window.currentRow)
w_read_at_dense_rank_site_id_b_3 = Window.partitionBy("read_at_dense_rank", "site_id").orderBy("voltage", "id")
w_read_at_dense_rank_site_id_b_4 = Window.partitionBy("read_at_dense_rank", "site_id").orderBy("voltage", "id").rowsBetween(-6, Window.currentRow)
w_read_at_dense_rank_site_id_b_5 = Window.partitionBy("read_at_dense_rank", "site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.max("voltage").over(w_quality_by_voltage_prior_1).alias("voltage_max_1_prior"),
        F.dense_rank().over(w_all_by_read_at).alias("read_at_dense_rank"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.rank().over(w_quality_by_voltage_ties).alias("voltage_rank"),
    )
)

with_trends = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w_read_at_dense_rank_site_id_b).alias("voltage_dense_rank"),
        F.max("voltage_rank").over(w_read_at_dense_rank_site_id_b_2).alias("voltage_rank_max_4"),
        F.rank().over(w_read_at_dense_rank_site_id_b_3).alias("voltage_rank_2"),
        F.max("kwh").over(w_read_at_dense_rank_site_id_b_4).alias("kwh_max_7"),
        F.last("rows_in_site_id").over(w_read_at_dense_rank_site_id_b_5).alias("last_rows_in_site_id"),
    )
)

result = (
    with_trends
    .groupBy("voltage_dense_rank")
    .pivot("quality", ["BAD", "EST", "OK"])
    .agg(F.count("voltage").alias("count_v"), F.sum("voltage").alias("sum_v"))
    .orderBy("voltage_dense_rank")
)
