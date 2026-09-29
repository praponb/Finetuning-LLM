from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_voltage_ties = Window.partitionBy("quality").orderBy("voltage")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_quality_by_voltage = Window.partitionBy("quality").orderBy("voltage", "id")
w_site_id = Window.partitionBy("site_id")
w_quality_by_voltage_full = Window.partitionBy("quality").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month_quality_by_kwh = Window.partitionBy(F.date_trunc("month", "read_at"), "quality").orderBy("kwh", "id").rowsBetween(-2, -1)
w_prev_read_at_to_date_read_at = Window.partitionBy(F.to_date("prev_read_at"), F.year("read_at"))
w_all_by_quality_site_id_avg_v = Window.orderBy("quality_site_id_avg_voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_meter_readings
    .filter((F.col("kwh") < 110.5) & (F.col("voltage") >= 222.3))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.percent_rank().over(w_quality_by_voltage_ties), 3).alias("voltage_percent_rank"),
        F.sum("kwh").over(w_quality_site_id).alias("quality_site_id_sum_kwh"),
        F.lag("voltage").over(w_quality_by_voltage).alias("prev_voltage"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.round(F.avg("voltage").over(w_quality_site_id), 2).alias("quality_site_id_avg_voltage"),
        F.min("kwh").over(w_quality_site_id).alias("quality_site_id_min_kwh"),
        F.nth_value("kwh", 2).over(w_quality_by_voltage_full).alias("second_kwh"),
        F.row_number().over(w_quality_by_voltage).alias("voltage_seq"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
        F.lag("read_at").over(w_quality_by_voltage).alias("prev_read_at"),
        F.max("voltage").over(w_read_at_month_quality_by_kwh).alias("voltage_max_2_prior"),
    )
)

annotated = (
    sequenced
    .select(
        "*",
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
        F.concat(F.col("quality"), F.lit("#"), F.col("voltage_seq").cast("string")).alias("quality_ref"),
    )
)

metrics = (
    annotated
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "voltage_percent_rank",
        "prev_voltage",
        "quality_site_id_avg_voltage",
        "voltage_seq",
        "voltage_vs_site_id_avg",
        "prev_read_at",
        "quality_clean",
        F.min("voltage_vs_site_id_avg").over(w_prev_read_at_to_date_read_at).alias("prev_read_at_to_date_read_at_year_min_voltage_vs"),
        F.min("kwh_vs_site_id_avg").over(w_prev_read_at_to_date_read_at).alias("prev_read_at_to_date_read_at_year_min_kwh_vs_sit"),
        F.nth_value("prev_voltage", 2).over(w_all_by_quality_site_id_avg_v).alias("second_prev_voltage"),
    )
)

result = (
    metrics
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "voltage_percent_rank",
        "quality_site_id_avg_voltage",
        "voltage_seq",
        "voltage_vs_site_id_avg",
        "prev_read_at",
        "quality_clean",
        "prev_read_at_to_date_read_at_year_min_voltage_vs",
        "prev_read_at_to_date_read_at_year_min_kwh_vs_sit",
        "second_prev_voltage",
        F.dayofweek(F.col("prev_read_at")).alias("prev_read_at_dayofweek"),
    )
    .orderBy("voltage")
)
