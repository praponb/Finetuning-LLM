from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month = Window.partitionBy(F.month("read_at"))
w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy("read_at")
w_all_by_kwh = Window.orderBy("kwh", "id")
w_quality_site_id_by_voltage_p = Window.partitionBy("quality", "site_id").orderBy("voltage_per_kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_voltage_plus_kwh = Window.partitionBy("site_id").orderBy("voltage_plus_kwh", "id")
w_quality = Window.partitionBy("quality")
w_all_by_kwh_tercile_ties = Window.orderBy("kwh_tercile")
w_kwh_tercile = Window.partitionBy("kwh_tercile")
w_site_id_by_read_at_rank = Window.partitionBy("site_id").orderBy("read_at_rank", "id")
w_distinct_quality_in_read_at = Window.partitionBy("distinct_quality_in_read_at_month")

filtered = (
    df_meter_readings
    .filter(F.col("voltage") < 229.1)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
)

with_trends = (
    filtered
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "voltage_plus_kwh",
        "voltage_per_kwh",
        F.max("voltage_plus_kwh").over(w_read_at_month).alias("read_at_month_max_voltage_plus_kwh"),
        F.size(F.collect_set("quality").over(w_read_at_month)).alias("distinct_quality_in_read_at_month"),
        F.rank().over(w_site_id_by_read_at_ties).alias("read_at_rank"),
        F.lag("voltage", 3).over(w_all_by_kwh).alias("prev3_voltage"),
        F.ntile(3).over(w_all_by_kwh).alias("kwh_tercile"),
        F.first("voltage_plus_kwh", ignorenulls=True).over(w_quality_site_id_by_voltage_p).alias("first_known_voltage_plus_kwh"),
        (F.col("voltage_per_kwh") - F.lag("voltage_per_kwh").over(w_site_id_by_voltage_plus_kwh)).alias("voltage_per_kwh_change"),
        F.max("voltage_per_kwh").over(w_quality).alias("quality_max_voltage_per_kwh"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
    )
)

sequenced = (
    with_trends
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_kwh_tercile_ties), 3).alias("kwh_tercile_percent_rank"),
        F.max("kwh").over(w_kwh_tercile).alias("kwh_tercile_max_kwh"),
        F.lead("rows_in_quality", 1, 0).over(w_site_id_by_read_at_rank).alias("next_rows_in_quality"),
        F.min("quality_max_voltage_per_kwh").over(w_distinct_quality_in_read_at).alias("distinct_quality_in_read_at_month_min_quality_ma"),
        F.lead("voltage_plus_kwh").over(w_site_id_by_read_at_rank).alias("next_voltage_plus_kwh"),
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
        "voltage_plus_kwh",
        "read_at_month_max_voltage_plus_kwh",
        "distinct_quality_in_read_at_month",
        "read_at_rank",
        "prev3_voltage",
        "kwh_tercile",
        "voltage_per_kwh_change",
        "quality_max_voltage_per_kwh",
        "rows_in_quality",
        "kwh_tercile_percent_rank",
        "kwh_tercile_max_kwh",
        "distinct_quality_in_read_at_month_min_quality_ma",
        "next_voltage_plus_kwh",
    )
    .orderBy(F.col("voltage").asc_nulls_last(), F.col("site_id").desc())
)
