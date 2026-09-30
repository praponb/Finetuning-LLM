from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy(F.col("voltage").desc())
w_site_id_by_voltage_running = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality_by_voltage = Window.partitionBy("site_id", "quality").orderBy("voltage", "id")
w_site_id_read_at_to_date = Window.partitionBy("site_id", F.to_date("read_at"))
w_voltage_rank_by_voltage_rank = Window.partitionBy("voltage_rank").orderBy(F.col("voltage_rank").desc(), "id").rowsBetween(-3, Window.currentRow)
w_voltage_rank_by_voltage_seq = Window.partitionBy("voltage_rank").orderBy("voltage_seq", "id")
w_voltage_rank_by_voltage_rank_2 = Window.partitionBy("voltage_rank").orderBy(F.col("voltage_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_voltage_rank = Window.partitionBy("site_id", "voltage_rank")
w_quality_read_at_month = Window.partitionBy("quality", F.date_trunc("month", "read_at"))
w_quality_by_voltage = Window.partitionBy("quality").orderBy("voltage", "id")
w_dedup = Window.partitionBy("quality").orderBy(F.col("max_kwh_so_far").desc(), F.col("id").desc())

filtered_readings = (
    df_meter_readings
    .filter(F.col("kwh").between(15.9, 96.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_trends = (
    filtered_readings
    .select(
        "*",
        F.rank().over(w_site_id_by_voltage_ties).alias("voltage_rank"),
        F.max("kwh").over(w_site_id_by_voltage_running).alias("max_kwh_so_far"),
        F.row_number().over(w_site_id_quality_by_voltage).alias("voltage_seq"),
        F.sum("voltage").over(w_site_id_read_at_to_date).alias("site_id_read_at_to_date_sum_voltage"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "voltage_rank",
        "max_kwh_so_far",
        "voltage_seq",
        F.round(F.avg("voltage_seq").over(w_voltage_rank_by_voltage_rank), 2).alias("voltage_seq_avg_4"),
        F.row_number().over(w_voltage_rank_by_voltage_seq).alias("rn"),
        F.nth_value("kwh", 2).over(w_voltage_rank_by_voltage_rank_2).alias("second_kwh"),
        F.count(F.lit(1)).over(w_site_id_voltage_rank).alias("rows_in_site_id_voltage_rank"),
    )
)

enriched = (
    metrics
    .filter(F.col("second_kwh") < 87.1)
    .select(
        "*",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.lead("max_kwh_so_far", 2, 0).over(w_quality_by_voltage).alias("next2_max_kwh_so_far"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_read_at_month), 2).alias("voltage_vs_quality_read_at_month_avg"),
    )
)

qualified = (
    windowed
    .crossJoin(windowed.agg(F.min("rn").alias("overall_min_rn")))
    .filter(F.col("rn") > F.col("overall_min_rn"))
    .drop("overall_min_rn")
)

deduped = (
    qualified
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "voltage_rank",
        "max_kwh_so_far",
        "voltage_seq",
        "voltage_seq_avg_4",
        "rn",
        "rows_in_site_id_voltage_rank",
        "quality_prefix",
        "voltage_vs_quality_read_at_month_avg",
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
    .orderBy(F.col("voltage_vs_quality_read_at_month_avg").desc_nulls_first())
)
