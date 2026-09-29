from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality = Window.partitionBy("site_id", "quality")
w_site_id_quality_by_read_at = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").desc_nulls_first(), "id")
w_read_at_to_date_by_kwh = Window.partitionBy(F.to_date("read_at")).orderBy(F.col("kwh").desc(), "id")
w_quality_by_kwh_minus_voltage = Window.partitionBy("quality").orderBy(F.col("kwh_minus_voltage").desc(), "id").rowsBetween(-2, Window.currentRow)
w_read_at_month_quality = Window.partitionBy(F.date_trunc("month", "read_at"), "quality")
w_quality_site_id_by_kwh_full = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_kwh_minus_voltage = Window.partitionBy("site_id").orderBy(F.col("kwh_minus_voltage").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_site_id_by_kwh_minus_voltage_2 = Window.partitionBy("site_id").orderBy(F.col("kwh_minus_voltage").desc_nulls_first(), "id")
w_quality_site_id_by_kwh = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc(), "id")
w_row_count_voltage_tier_by_mi = Window.partitionBy("row_count", "voltage_tier").orderBy("min_voltage")
w_read_at_month_month_row_coun = Window.partitionBy(F.date_trunc("month", "read_at_month"), "row_count")
w_row_count_voltage_tier_by_mi_2 = Window.partitionBy("row_count", "voltage_tier").orderBy("min_voltage", "read_at_month", "voltage_tier")

prepared = (
    df_meter_readings
    .filter((F.col("voltage") <= 233.6) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
    )
)

sequenced = (
    prepared
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "kwh_minus_voltage",
        F.sum("kwh").over(w_site_id_quality).alias("site_id_quality_sum_kwh"),
        F.lead("read_at", 3).over(w_site_id_quality_by_read_at).alias("next3_read_at"),
        F.lag("kwh_minus_voltage", 3).over(w_read_at_to_date_by_kwh).alias("prev3_kwh_minus_voltage"),
        (F.col("kwh") - F.lag("kwh").over(w_site_id_quality_by_read_at)).alias("kwh_change"),
        F.round(F.avg("kwh").over(w_quality_by_kwh_minus_voltage), 2).alias("kwh_avg_3"),
        F.count(F.lit(1)).over(w_read_at_month_quality).alias("rows_in_read_at_month_quality"),
        F.max("kwh_minus_voltage").over(w_read_at_month_quality).alias("read_at_month_quality_max_kwh_minus_voltage"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id_quality), 2).alias("kwh_vs_site_id_quality_avg"),
        F.first("kwh_minus_voltage").over(w_quality_site_id_by_kwh_full).alias("first_kwh_minus_voltage"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id_quality), 2).alias("voltage_vs_site_id_quality_avg"),
        F.max("kwh_minus_voltage").over(w_site_id_by_kwh_minus_voltage).alias("kwh_minus_voltage_max_3"),
        F.size(F.collect_set("site_id").over(w_read_at_month_quality)).alias("distinct_site_id_in_read_at_month_quality"),
        F.row_number().over(w_site_id_by_kwh_minus_voltage_2).alias("kwh_minus_voltage_seq"),
        F.row_number().over(w_read_at_to_date_by_kwh).alias("rn"),
        F.lead("read_at").over(w_quality_site_id_by_kwh).alias("next_read_at"),
    )
)

grouped = (
    sequenced
    .groupBy(
        F.date_trunc("month", "read_at").alias("read_at_month"),
        F.when(F.col("voltage") >= 232.4, "large").when(F.col("voltage") >= 224.8, "medium").otherwise("small").alias("voltage_tier"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("quality") == "EST", 1)) / F.count("*"), 1).alias("pct_quality_est"),
        F.min("voltage").alias("min_voltage"),
        F.round(F.sum("kwh_minus_voltage_seq") / F.nullif(F.sum("kwh_avg_3"), F.lit(0)), 3).alias("kwh_minus_voltage_seq_to_kwh_avg_3_ratio"),
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
)

metrics = (
    grouped
    .select(
        "*",
        F.rank().over(w_row_count_voltage_tier_by_mi).alias("min_voltage_rank"),
        F.count(F.lit(1)).over(w_read_at_month_month_row_coun).alias("rows_in_read_at_month_month_row_count"),
        F.row_number().over(w_row_count_voltage_tier_by_mi_2).alias("min_voltage_seq"),
    )
)

result = (
    metrics
    .filter(F.col("min_voltage_seq") <= 9)
    .select(
        "read_at_month",
        "pct_quality_est",
        "min_voltage",
        "kwh_minus_voltage_seq_to_kwh_avg_3_ratio",
        "row_count",
        "min_voltage_rank",
        "rows_in_read_at_month_month_row_count",
        "min_voltage_seq",
        F.to_date(F.col("read_at_month")).alias("read_at_month_date"),
    )
    .orderBy(F.col("read_at_month_date").desc())
)
