from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_site_id_by_kwh_running = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_to_date_by_kwh_runni = Window.partitionBy(F.to_date("read_at")).orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id")
w_all_by_voltage_running = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_voltage = Window.orderBy("voltage", "id")
w_read_at_to_date_by_kwh_ties = Window.partitionBy(F.to_date("read_at")).orderBy("kwh")

valid = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

metrics = (
    valid
    .select(
        "*",
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.last("kwh", ignorenulls=True).over(w_site_id_by_kwh_running).alias("last_known_kwh"),
        F.count(F.lit(1)).over(w_read_at_to_date_by_kwh_runni).alias("running_count"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality), F.lit(0)), 4).alias("voltage_share_of_quality"),
        F.sum("kwh").over(w_quality).alias("quality_sum_kwh"),
        F.round(F.percent_rank().over(w_site_id_by_kwh), 3).alias("kwh_percent_rank"),
        F.round(F.avg("voltage").over(w_quality), 2).alias("quality_avg_voltage"),
        F.row_number().over(w_site_id_by_kwh).alias("kwh_seq"),
        F.max("voltage").over(w_all_by_voltage_running).alias("max_voltage_so_far"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.dense_rank().over(w_all_by_voltage).alias("voltage_dense_rank"),
        F.round(F.cume_dist().over(w_read_at_to_date_by_kwh_ties), 3).alias("kwh_cume_dist"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "distinct_site_id_in_quality",
        "voltage_share_of_quality",
        "kwh_percent_rank",
        "quality_avg_voltage",
        "kwh_seq",
        "rows_in_quality",
        "voltage_dense_rank",
        "kwh_cume_dist",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
    )
)

result = (
    scored
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "distinct_site_id_in_quality",
        "voltage_share_of_quality",
        "kwh_seq",
        "rows_in_quality",
        "kwh_cume_dist",
        "read_at_quarter",
        F.round(F.col("kwh_seq") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("kwh_seq_per_kwh"),
        F.greatest(F.col("kwh_seq"), F.col("read_at_quarter")).alias("greatest_kwh_seq_read_at_quarter"),
    )
    .orderBy(F.col("kwh_seq").asc(), "id")
    .limit(20)
)
