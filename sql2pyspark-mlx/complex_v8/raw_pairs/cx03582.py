from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_voltage_p = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_quality_site_id_by_voltage = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage").desc_nulls_first(), "id")
w_all_by_days_since_read_at_ru = Window.orderBy(F.col("days_since_read_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_id_by_read_at_r = Window.partitionBy("quality", "site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_days_since_read_a = Window.partitionBy("quality").orderBy("days_since_read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_quality_by_days_since_read_a_2 = Window.partitionBy("quality").orderBy("days_since_read_at", "id")
w_quality_by_days_since_read_a_3 = Window.partitionBy("quality").orderBy("days_since_read_at")
w_all_by_days_since_read_at = Window.orderBy(F.col("days_since_read_at").desc_nulls_first(), "id")
w_read_at_year_site_id_by_days = Window.partitionBy(F.year("read_at"), "site_id").orderBy("days_since_read_at")
w_quality_site_id_by_voltage_t = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage").desc_nulls_first())
w_all_by_days_since_read_at_ti = Window.orderBy(F.col("days_since_read_at").desc_nulls_first())
w_voltage_seq = Window.partitionBy("voltage_seq")
w_site_id_by_running_days_sinc = Window.partitionBy("site_id").orderBy("running_days_since_read_at", "id")

cleaned = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(10, 13)) & (F.col("voltage") >= 219.6))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        F.round(F.avg("days_since_read_at").over(w_quality_site_id_by_voltage_p), 2).alias("days_since_read_at_avg_3_prior"),
        F.row_number().over(w_quality_site_id_by_voltage).alias("voltage_seq"),
        F.first("voltage", ignorenulls=True).over(w_all_by_days_since_read_at_ru).alias("first_known_voltage"),
        F.sum("days_since_read_at").over(w_quality_site_id_by_read_at_r).alias("running_days_since_read_at"),
        (F.col("voltage") - F.lag("voltage").over(w_quality_site_id_by_voltage)).alias("voltage_change"),
        F.last("days_since_read_at").over(w_quality_by_days_since_read_a).alias("last_days_since_read_at"),
        F.min("kwh").over(w_site_id_quality).alias("site_id_quality_min_kwh"),
        F.ntile(4).over(w_quality_by_days_since_read_a_2).alias("days_since_read_at_quartile"),
        F.rank().over(w_quality_by_days_since_read_a_3).alias("days_since_read_at_rank"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_days_since_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.cume_dist().over(w_read_at_year_site_id_by_days), 3).alias("days_since_read_at_cume_dist"),
        F.rank().over(w_read_at_year_site_id_by_days).alias("days_since_read_at_rank_2"),
        F.round(F.cume_dist().over(w_quality_site_id_by_voltage_t), 3).alias("voltage_cume_dist"),
        F.dense_rank().over(w_all_by_days_since_read_at_ti).alias("days_since_read_at_dense_rank"),
    )
)

tagged = (
    metrics
    .select(
        "*",
        F.round(F.col("voltage_seq") / F.nullif(F.col("last_days_since_read_at"), F.lit(0)), 3).alias("voltage_seq_per_last_days_since_read_at"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        F.size(F.collect_set("site_id").over(w_voltage_seq)).alias("distinct_site_id_in_voltage_seq"),
        F.max("voltage_cume_dist").over(w_voltage_seq).alias("voltage_seq_max_voltage_cume_dist"),
        (F.col("days_since_read_at_quartile") - F.lag("days_since_read_at_quartile").over(w_site_id_by_running_days_sinc)).alias("days_since_read_at_quartile_change"),
        F.round(F.avg("minutes_since_prev_read_at").over(w_voltage_seq), 2).alias("voltage_seq_avg_minutes_since_prev_read_at"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "voltage",
        "days_since_read_at_avg_3_prior",
        "voltage_seq",
        "first_known_voltage",
        "running_days_since_read_at",
        "site_id_quality_min_kwh",
        "minutes_since_prev_read_at",
        "days_since_read_at_cume_dist",
        "days_since_read_at_rank_2",
        "voltage_cume_dist",
        "days_since_read_at_dense_rank",
        "voltage_seq_per_last_days_since_read_at",
        "distinct_site_id_in_voltage_seq",
        "voltage_seq_max_voltage_cume_dist",
        "days_since_read_at_quartile_change",
        "voltage_seq_avg_minutes_since_prev_read_at",
    )
    .orderBy("id")
)
