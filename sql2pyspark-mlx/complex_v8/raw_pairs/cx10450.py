from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_month = Window.partitionBy(F.month("read_at_month"))
w_row_count_by_min_kwh_ties = Window.partitionBy("row_count").orderBy("min_kwh")
w_last_read_at_month_by_last_r = Window.partitionBy(F.date_trunc("month", "last_read_at")).orderBy("last_read_at", "read_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_read_at_month_by_last_r_2 = Window.partitionBy(F.date_trunc("month", "last_read_at")).orderBy("last_read_at", "read_at_month")

cleaned_readings = (
    df_meter_readings
    .filter(F.col("site_id") >= 3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

summary = (
    cleaned_readings
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.min("kwh").alias("min_kwh"),
        F.max("read_at").alias("last_read_at"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.max("days_since_read_at").alias("max_days_since_read_at_check"),
    )
    .filter(F.col("max_days_since_read_at_check") > 70)
    .drop("max_days_since_read_at_check")
)

ranked = (
    summary
    .select(
        "*",
        F.max("row_count").over(w_read_at_month_month).alias("read_at_month_month_max_row_count"),
        F.round(F.cume_dist().over(w_row_count_by_min_kwh_ties), 3).alias("min_kwh_cume_dist"),
        F.sum("row_count").over(w_last_read_at_month_by_last_r).alias("running_row_count"),
        F.ntile(4).over(w_last_read_at_month_by_last_r_2).alias("last_read_at_quartile"),
        F.count(F.lit(1)).over(w_read_at_month_month).alias("rows_in_read_at_month_month"),
    )
)

result = (
    ranked
    .filter(F.col("last_read_at_quartile") > 3)
    .select(
        "read_at_month",
        "row_count",
        "last_read_at",
        "read_at_month_month_max_row_count",
        "running_row_count",
        "rows_in_read_at_month_month",
        F.date_format(F.col("last_read_at"), "MMM yyyy").alias("last_read_at_label"),
        F.round(F.col("min_kwh") * 0.25, 2).alias("min_kwh_adj"),
    )
    .orderBy(F.col("running_row_count").desc())
)
