from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_to_date = Window.partitionBy(F.to_date("read_at"))
w_site_id_quality_by_voltage_t = Window.partitionBy("site_id", "quality").orderBy("voltage")
w_quality_site_id_by_voltage = Window.partitionBy("quality", "site_id").orderBy("voltage", "id")
w_all_by_read_at_full = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_site_id_read_at_to_date_by_r = Window.partitionBy("site_id", F.to_date("read_at")).orderBy(F.col("read_at").desc(), "id")
w_all_by_read_at_ties = Window.orderBy("read_at")
w_site_id_quality_by_voltage_r = Window.partitionBy("site_id", "quality").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_id_by_voltage_t = Window.partitionBy("quality", "site_id").orderBy("voltage")
w_site_id_read_at_dense_rank_2 = Window.partitionBy("site_id", "read_at_dense_rank_2")
w_read_at_dense_rank_2_read_at = Window.partitionBy("read_at_dense_rank_2", "read_at_dense_rank_3")
w_all_by_next_read_at_running = Window.orderBy("next_read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_quintile_by_running = Window.partitionBy("read_at_quintile").orderBy("running_voltage", "id")
w_voltage_dense_rank_2_by_read = Window.partitionBy("voltage_dense_rank_2").orderBy("read_at_dense_rank_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_quintile_by_running_2 = Window.partitionBy("read_at_quintile").orderBy("running_voltage", "id").rowsBetween(-2, Window.currentRow)
w_read_at_dense_rank_filled_by = Window.partitionBy("read_at_dense_rank_filled").orderBy("voltage_dense_rank", "id")

base = (
    df_meter_readings
    .filter((F.col("voltage") <= 234.5) | (F.col("quality").isin("EST", "OK")))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

sequenced = (
    base
    .select(
        "*",
        F.round(F.col("voltage") - F.avg("voltage").over(w_read_at_to_date), 2).alias("voltage_vs_read_at_to_date_avg"),
        F.dense_rank().over(w_site_id_quality_by_voltage_t).alias("voltage_dense_rank"),
        (F.col("voltage") - F.lag("voltage").over(w_quality_site_id_by_voltage)).alias("voltage_change"),
        F.dense_rank().over(w_quality_site_id_by_voltage).alias("voltage_dense_rank_2"),
        F.last("voltage").over(w_all_by_read_at_full).alias("last_voltage"),
        F.lead("read_at").over(w_quality_site_id_by_voltage).alias("next_read_at"),
        F.dense_rank().over(w_all_by_read_at).alias("read_at_dense_rank"),
        F.ntile(5).over(w_site_id_read_at_to_date_by_r).alias("read_at_quintile"),
        F.dense_rank().over(w_site_id_read_at_to_date_by_r).alias("read_at_dense_rank_2"),
        F.dense_rank().over(w_all_by_read_at_ties).alias("read_at_dense_rank_3"),
        F.row_number().over(w_quality_site_id_by_voltage).alias("seq_no"),
        F.sum("voltage").over(w_site_id_quality_by_voltage_r).alias("running_voltage"),
        F.round(F.percent_rank().over(w_site_id_quality_by_voltage_t), 3).alias("voltage_percent_rank"),
        F.dense_rank().over(w_quality_site_id_by_voltage_t).alias("voltage_dense_rank_3"),
    )
)

labeled = (
    sequenced
    .filter(F.col("read_at") >= F.lit("2025-02-06 13:00:00").cast("timestamp"))
    .withColumn("read_at_dense_rank_filled", F.coalesce(F.col("read_at_dense_rank"), F.lit(0)))
)

windowed = (
    labeled
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "voltage_dense_rank",
        "voltage_dense_rank_2",
        "last_voltage",
        "next_read_at",
        "read_at_dense_rank",
        "read_at_dense_rank_2",
        "read_at_dense_rank_3",
        "seq_no",
        "voltage_percent_rank",
        F.round(F.col("read_at_quintile") / F.nullif(F.sum("read_at_quintile").over(w_site_id_read_at_dense_rank_2), F.lit(0)), 4).alias("read_at_quintile_share_of_site_id_read_at_dense_"),
        F.count(F.lit(1)).over(w_read_at_dense_rank_2_read_at).alias("rows_in_read_at_dense_rank_2_read_at_dense_rank_"),
        F.last("voltage", ignorenulls=True).over(w_all_by_next_read_at_running).alias("last_known_voltage"),
        F.max("read_at_dense_rank_3").over(w_read_at_dense_rank_2_read_at).alias("read_at_dense_rank_2_read_at_dense_rank_3_max_re"),
        F.lag("seq_no").over(w_read_at_quintile_by_running).alias("prev_seq_no"),
        F.first("read_at_dense_rank", ignorenulls=True).over(w_voltage_dense_rank_2_by_read).alias("first_known_read_at_dense_rank"),
        F.sum("voltage_percent_rank").over(w_read_at_quintile_by_running_2).alias("voltage_percent_rank_sum_3"),
        F.lead("last_voltage", 1, 0).over(w_read_at_dense_rank_filled_by).alias("next_last_voltage"),
    )
)

totals = (
    windowed
    .groupBy("read_at_dense_rank_2")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("last_voltage"), 2).alias("total_last_voltage"),
        F.countDistinct("quality").alias("distinct_qualitys"),
    )
)

result = (
    totals
    .filter(F.col("distinct_qualitys") <= 6)
    .select(
        "read_at_dense_rank_2",
        "cnt",
        F.least(F.col("read_at_dense_rank_2"), F.col("total_last_voltage")).alias("least_read_at_dense_rank_2_total_last_voltage"),
        F.round(F.col("total_last_voltage") * 0.9, 2).alias("total_last_voltage_adj"),
    )
    .orderBy(F.col("total_last_voltage_adj").asc_nulls_last())
)
