from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_least_voltage_kwh_to = Window.orderBy("least_voltage_kwh_to_voltage_ratio", "read_at_month", "read_at_quarter", "quality")
w_quality_read_at_quarter_by_l = Window.partitionBy("quality", "read_at_quarter").orderBy(F.col("least_voltage_kwh_to_voltage_ratio").desc(), "read_at_month", "read_at_quarter", "quality").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_rows_by_min_least_voltage = Window.partitionBy("n_rows").orderBy(F.col("min_least_voltage_kwh").desc(), "read_at_month", "read_at_quarter", "quality").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_rows_by_min_least_voltage_2 = Window.partitionBy("n_rows").orderBy(F.col("min_least_voltage_kwh").desc(), "read_at_month", "read_at_quarter", "quality")

cleaned_readings = (
    df_meter_readings
    .filter(F.col("voltage") > 222.6)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
    )
)

grouped = (
    cleaned_readings
    .filter(F.col("least_voltage_kwh") > 85.8)
    .groupBy(
        F.date_trunc("month", "read_at").alias("read_at_month"),
        "read_at_quarter",
        "quality",
    )
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.count("*").alias("n_rows"),
        F.min("least_voltage_kwh").alias("min_least_voltage_kwh"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("read_at_quarter").alias("total_read_at_quarter"),
        F.round(F.sum("least_voltage_kwh") / F.nullif(F.sum("voltage"), F.lit(0)), 3).alias("least_voltage_kwh_to_voltage_ratio"),
        F.sum("least_voltage_kwh").alias("sum_least_voltage_kwh_check"),
    )
    .filter(F.col("sum_least_voltage_kwh_check") > 24.5)
    .drop("sum_least_voltage_kwh_check")
)

windowed = (
    grouped
    .select(
        "read_at_month",
        "read_at_quarter",
        "quality",
        "quality_list",
        "n_rows",
        "min_least_voltage_kwh",
        "distinct_ids",
        "total_read_at_quarter",
        F.row_number().over(w_all_by_least_voltage_kwh_to).alias("rn"),
        F.first("n_rows").over(w_quality_read_at_quarter_by_l).alias("first_n_rows"),
    )
)

ranked = (
    windowed
    .select(
        "read_at_month",
        "read_at_quarter",
        "quality",
        "quality_list",
        "n_rows",
        "min_least_voltage_kwh",
        "distinct_ids",
        "total_read_at_quarter",
        F.first("rn", ignorenulls=True).over(w_n_rows_by_min_least_voltage).alias("first_known_rn"),
        F.rank().over(w_n_rows_by_min_least_voltage_2).alias("min_least_voltage_kwh_rank"),
    )
)

result = (
    ranked
    .groupBy("first_known_rn", "read_at_quarter")
    .pivot("quality", ["OK", "BAD", "EST"])
    .agg(F.count("min_least_voltage_kwh"))
    .orderBy("first_known_rn", "read_at_quarter")
)
